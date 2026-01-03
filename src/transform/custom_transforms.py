from pyspark.sql.types import *
from pyspark.sql.functions import *
from utils.table_loader import read_jdbc, read_clickhouse
import phonenumbers
from phonenumbers.phonenumberutil import NumberParseException
from typing import List

# Joins will make the right table columns have right_table_name + __ + right_table_column
# Add replace columns to the join

def perform_join(spark, schema, df, join_conf):

    # right_table_name is the name of the table joining df.
    right_table_name = join_conf['with_table']

    # decides where to retrieve the right table, either from clickhouse or from SOURCE.
    if 'clickhouse' in join_conf:
        right_df = read_clickhouse(spark, schema, right_table_name)
    else:
        right_df = read_jdbc(spark, schema, right_table_name)

    # converts all of right table's column names to have a prefix {right_table_name}__
    right_df = right_df.select([col(c).alias(f"{right_table_name}__{c}") for c in right_df.columns])

    # predicates for the join.
    how = join_conf['how']
    left_on = join_conf['left']
    right_on = right_table_name + '__' + join_conf['right']
    
    # a left-joined df with the right table. 
    joined_df = df.join(
        right_df,
        df[left_on]==right_df[right_on],
        how=how
    )
    
    # columns from the right table that are being kept in the df after join. 
    if 'right_table_columns' in join_conf:
        right_cols = [right_table_name + '__' + s for s in join_conf['right_table_columns']]
    else:
        right_cols = []

    # removes rows with certain conditions.
    if 'filter_rows' in join_conf:
        for key, value in join_conf['filter_rows'].items():
            joined_df = joined_df.filter(col(key) == col(f'{right_table_name}__{value}'))
    
    # final table with the left table columns and right table columns desired.
    left_cols = df.columns
    final_cols = [col(c) for c in left_cols + right_cols]
    joined_df = joined_df.select(*final_cols)

    return joined_df

# Function to normalize phone numbers in a DataFrame column.

def normalize_phone_number(df, column):
    c = col(column)

    # 1) Canonicalize to string, trim, and catch empties/zeros/junk early
    cleaned = trim(c.cast("string"))

    cleaned = when(
        cleaned.isNull() |
        (cleaned == "") |
        (trim(cleaned) == "") |
        (trim(cleaned) == "0") |
        (cleaned.rlike("(?i)\\be\\+?\\d+")) |   # any E/e+digits pattern
        (cleaned.rlike("(?i)\\bn/?a\\b")) |     # N/A variants
        (cleaned.rlike("(?i)^null$")),          # "NULL"
        None
    ).otherwise(cleaned)

    # 2) Strip EVERYTHING that isn't a digit (handles +, (), ., /, spaces, hyphens, etc.)
    cleaned = when(
        cleaned.isNotNull(),
        regexp_replace(cleaned, "[^0-9]", "")
    ).otherwise(None)

    # 3) Normalize Indonesian leading forms:
    #    62xxxxxxxxxx -> 0xxxxxxxxxx
    #    8xxxxxxxxxx  -> 08xxxxxxxxx
    cleaned = when(
        cleaned.isNotNull() & cleaned.startswith("62"),
        regexp_replace(cleaned, "^62", "0")
    ).when(
        cleaned.isNotNull() & cleaned.startswith("8"),
        concat(lit("0"), cleaned)
    ).otherwise(cleaned)

    # 4) Final digits-only sanity (after normalization this should pass)
    cleaned = when(cleaned.rlike("^[0-9]+$"), cleaned).otherwise(None)

    return df.withColumn(f'{column}_normalized', cleaned)

# Function to cross-check validity of format of Indonesian phone numbers using the phonenumbers library.

def validate_phone_number(df, column):

    def validate_phone_id(num):
        if not num:
            return False
        try:
            pn = phonenumbers.parse(num, "ID")
            return True if phonenumbers.is_valid_number(pn) else False
        except NumberParseException:
            return False
        
    validate_phone_udf = udf(validate_phone_id, BooleanType())
    df = df.withColumn(f'{column}_valid', validate_phone_udf(col(column)))
    return df

# Function to determine phone number status based on raw and normalized validity.
# valid if both are True, check if both are False, else invalid.

def phone_number_status(df, column, normalized_column):

    valid_col = f"{column}_valid"
    valid_normal_col = f"{normalized_column}_valid"
    status_col = f"{column}_status"

    return df.withColumn(
        status_col,
        when( col(valid_col) & col(valid_normal_col), lit("valid"))
        .when(~col(valid_col) & ~col(valid_normal_col), lit("invalid"))
        .otherwise(lit("check"))
    )

def clean_address(
    df: DataFrame, 
    address_cols: List[str], 
) -> DataFrame:

    # Trim + InitCap each address column
    for c in address_cols:
        df = df.withColumn(
            c,
            when(col(c).isNotNull(), initcap(trim(col(c)))).otherwise(None)
        )

    # Convert empty strings to null so concat_ws will skip them
    for c in address_cols:
        df = df.withColumn(
            c,
            when(length(trim(col(c))) > 0, col(c)).otherwise(None)
        )

    # Removes duplicates; keep the first non-blank occurrence; later identical values become NULL
    for i in range(len(address_cols)):
        base = address_cols[i]
        for j in range(i + 1, len(address_cols)):
            other = address_cols[j]
            df = df.withColumn(
                other,
                when(
                    col(base).isNotNull() & col(other).isNotNull() & (col(base) == col(other)),
                    lit(None)  # blank out duplicate
                ).otherwise(col(other))
            )

    return df

def clean_name(df, col_name):
    """
    Cleans and formats a name column:
    - Removes digits and unwanted special characters
    - Trims leading/trailing and multiple spaces
    - Converts to proper case (InitCap)
    - Returns None if the value is empty after cleaning
    """
    cleaned = (
        df.withColumn(
            col_name,
            # 1. Remove digits and unwanted symbols (keep letters, apostrophes, hyphens, and spaces)
            regexp_replace(col(col_name), r"[^A-Za-zÀ-ÿ'\-\s]", "")
        )
        # 2. Collapse multiple spaces into one
        .withColumn(col_name, regexp_replace(col(col_name), r"\s+", " "))
        # 3. Trim whitespace
        .withColumn(col_name, trim(col(col_name)))
        # 4. Convert to proper case
        .withColumn(col_name, initcap(col(col_name)))
        # 5. Set empty strings to null
        .withColumn(
            col_name,
            when(length(trim(col(col_name))) > 0, col(col_name)).otherwise(None)
        )
    )
    return cleaned

def clean_nik(df, col_name="nik"):
    """
    Cleans Indonesian ID (NIK) values:
    - Removes whitespace and non-digit characters
    - Trims leading/trailing spaces
    - Keeps only values that are exactly 16 digits
    - Invalid or empty values are set to NULL
    """

    cleaned_col = trim(col(col_name))                        # trim whitespace
    cleaned_col = regexp_replace(cleaned_col, r"\s+", "")     # remove spaces within
    cleaned_col = regexp_replace(cleaned_col, r"\D", "")      # remove non-digits

    cleaned = df.withColumn(
        col_name,
        when(length(cleaned_col) == 16, cleaned_col)          # keep only valid 16-digit numbers
        .otherwise(None)
    )
    return cleaned

def clean_postal_code(df, postal_code_col):
    """
    Cleans postal code column:
    - Converts to string
    - Trims whitespace
    - Keeps only 5-digit numeric postal codes
    - Invalid or empty values are set to NULL
    """

    cleaned_col = trim(col(postal_code_col).cast("string"))          # convert to string and trim whitespace
    cleaned_col = regexp_replace(cleaned_col, r"\s+", "")            # remove spaces within

    cleaned = df.withColumn(
        postal_code_col,
        when(cleaned_col.rlike(r"^\d{5}$"), cleaned_col)            # keep only valid 5-digit postal codes
        .otherwise(None)
    )
    return cleaned

def clean_coordinates(df, lat_col, lon_col):
    """
    Cleans latitude and longitude columns:
    - Converts to float
    - Sets invalid values (out of range) to NULL
    """

    cleaned_df = df.withColumn(
        lat_col,
        when(
            (col(lat_col).cast("float").isNotNull()) &
            (col(lat_col).cast("float") >= -90) &
            (col(lat_col).cast("float") <= 90),
            col(lat_col).cast("float")
        ).otherwise(None)
    ).withColumn(
        lon_col,
        when(
            (col(lon_col).cast("float").isNotNull()) &
            (col(lon_col).cast("float") >= -180) &
            (col(lon_col).cast("float") <= 180),
            col(lon_col).cast("float")
        ).otherwise(None)
    )

    # Additionally, set both lat and lon to NULL if lon is exactly 0.0. Because in Indonesia, Longitude is never 0.
    cleaned_df = cleaned_df.withColumn(
        lat_col, 
        when(col(lon_col) == 0.0, None).otherwise(col(lat_col))
    ).withColumn(
        lon_col,
        when(col(lon_col) == 0.0, None).otherwise(col(lon_col))
    )

    return cleaned_df

def clean_boolean_col(df, boolean_col):
    """
    Cleans a boolean column by:
    - Converting 'Y', 'Yes', 'True', '1' (case-insensitive) to True
    - Converting 'N', 'No', 'False', '0' (case-insensitive) to False
    - Setting invalid or empty values to NULL
    """

    cleaned = df.withColumn(
        boolean_col,
        when(
            lower(trim(col(boolean_col))).isin("y", "yes", "true", "1"),
            True
        ).when(
            lower(trim(col(boolean_col))).isin("n", "no", "false", "0"),
            False
        ).otherwise(None)
    )
    return cleaned

def clean_padded_char(
    df: DataFrame, 
    char_cols: List[str], 
    valid_values: List[str]
    ) -> DataFrame:
    """
    Cleans a bpchar(n) column by setting values not in valid_values to NULL.
    """

    # Convert valid_values to uppercase once for efficiency
    valid_values_upper = [v.upper() for v in valid_values] if valid_values else []

    cleaned_df = df
    for char_col in char_cols:
        cleaned_df = cleaned_df.withColumn(
            char_col,
            when(
                ~upper(col(char_col)).isin(valid_values_upper), 
                lit(None)
            ).otherwise(col(char_col))
        )
    
    return cleaned_df

def clean_string_location(df, string_col):
    """
    Cleans a string column by:
    - Trimming leading/trailing whitespace
    - Converting empty strings to NULL
    - Capitalizing the first letter of each word
    - Removing non-alphabetic characters except spaces, hyphens, and apostrophes
    """

    cleaned = df.withColumn(
        string_col,
        initcap(
            trim(
                regexp_replace(col(string_col), "[^A-Za-z\\s\\-']", "")))
    )
    
    return cleaned