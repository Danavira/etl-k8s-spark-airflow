from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql.functions import *
from transform.custom_transforms import *

def clean_nexseller_sales_invoice_item(df):

    # Fixes line discount that is actually line discount amount. (if line discount > 80, it is actually line discount amount).
    # for x in range(1,6):
    #     ld = f'line_discount_{x}'
    #     lda = f'line_discount_amount_{x}'
    #     df = df.withColumn(
    #                 ld,
    #                 when((col(ld) > 80) & (round(col(ld), 2) == round(col(lda), 2)), 
    #                      round(100 * col(ld) / (col("qty_sold")*col("selling_price")/col("conversion_1_to_4")), 2))
    #                 .otherwise(round(col(ld), 2)))

    df = clean_boolean_col(df, 'is_no_regular_discount')
    df = clean_boolean_col(df, 'is_embalace_item')
    df = clean_boolean_col(df, 'is_tax_1_applied')
    df = clean_boolean_col(df, 'is_tax_2_applied')
    df = clean_boolean_col(df, 'is_tax_3_applied')
    df = clean_boolean_col(df, 'is_new_sku')

    # Fixes the calculation of line discount based. use this or just replace with tax based 1.
    # df = df.withColumn(
    #     "line_discount_based",
    #     col("line_gross_amount") - (
    #         col("line_discount_amount_1") +
    #         col("line_discount_amount_2") +
    #         col("line_discount_amount_3") +
    #         col("line_discount_amount_4") +
    #         col("line_discount_amount_5")
    #     ))

    #Adds a column true conversion, to find the true conversion_1_to_4 based on selling price, order qty, and line discount based.
    # df = df.withColumn(
    #     'true_conversion',
    #     when(
    #         col("qty_order") != 0,
    #         col("selling_price") / (
    #             (col("line_discount_amount_1") + 
    #              col("line_discount_amount_2") +
    #              col("line_discount_amount_3") + 
    #              col("line_discount_amount_4") +
    #              col("line_discount_amount_5") + 
    #              col("line_discount_based")) / col("qty_order")
    #         )
    #     ).otherwise(None))

    # Adds tax to line net amount. Line net amount was already calculated with discounts. Now, it will also count tax.
    # df = df.withColumn(
    #     'line_net_amount',
    #     col('line_net_amount') + 
    #     col('tax_amount_1') + 
    #     col('tax_amount_2') + 
    #     col('tax_amount_3'))
    
    # Fixes selling price when it's 0 or null
    # df = df.withColumn(
    #     'selling_price',
    #     when(
    #         ((col("selling_price") == 0) | (col("selling_price").isNull())) &
    #         (col("line_gross_amount").isNotNull()) &
    #         (col("line_gross_amount") != 0) &
    #         (col("qty_order").isNotNull()) &
    #         (col("qty_order") != 0),
    #         col("line_gross_amount") * col("conversion_1_to_4") / col("qty_order")
    #     ).otherwise(col("selling_price")))

    # Fixes qty sold when it's 0 or null
    # df = df.withColumn(
    #     'qty_sold',
    #     when(
    #         ((col("qty_sold") == 0) | (col("qty_sold").isNull())) &
    #         (col("line_gross_amount").isNotNull()) &
    #         (col("line_gross_amount") != 0) &
    #         (col("selling_price").isNotNull()) &
    #         (col("selling_price") != 0),
    #         col("line_gross_amount") * col("conversion_1_to_4") / col("selling_price")
    #     ).otherwise(col("qty_sold")))
    
    # remove outdated product, (dont remove outdated products from the data warehouse. just filter it later on in the sre repo)
    # fix invoices that are not matching to the sum of invoice items.

    

    # prices (incl selling price) needs a uniform precision. 2 dp, 0 dp. 

    ## fix nexseller sales invoice id being at 0.
    # fix invoice items with qty order and qty sold at 0 and qty free good at 0, but still having a line gross amount.
    # fix selling price being at 0 and qty_order not at 0.
    
    # is there one general formula that fills the empty values in the equation?
    #LIST OF POSSIBLE FORMULAS (FOR VALIDATION)
    # LGA = (qty_order/conversion_1_to_4) * selling_price of uom_1
    # LGA - sum(line_discount_amounts) = line_discount_based (LGA is the raw price * qty)
    # line_discount_x * (LGA * (100-line_discount_[x-1]) = line_discount_amount_x
    # line_discount_based = LGA * (100-line_discount_1) * (100-line_discount_2) * (100-line_discount_3)
    # line_net_amount should have tax added. Line net amount = line net amount + tax amount 1 + tax amount 2
    # total cost of sales invoice items should match with the respective sales invoice.
    
    
    # FIX LINE DISCOUNT BASED BEING AT 0.
    # count of line_discount_based == tax_based_1 is 52,000,000. around 85%. 
    # tax_based_1 is actually line_discount_based - sum of discount amounts. (NOT LINE DISCOUNT AMOUNTS)
    
    ## add tax amount to line net amount.
    
    # does line_discount_based always equate to tax based 1? yes, with a max difference of 0.1
    
    # if discount amount 1,2,3 not 0, is_no_regular_discount has to be yes?
    
    # line_discount_based is equal to tax_based + discount amounts. but the discounts are not counted in line gross amount and 
    # line net amounts, which make sense, they are not line discounts.
    
    # some line gross amounts are not equal to line net amounts?? line gross amounts are only equal to line net amounts when 
    # there are no discounts, therefore line net amounts are already after discounts, and they should be after tax as well.

    # QTY free good is not always accurate. a lot of free good items get put in qty order and qty sold. 

    # there are only 16 million distinct sales invoice ids in the table, while there are 47 million sales invoices in existence.
    # there are a few invoice items that dont have a valid nexseller product id when joined to the nexseller product table.
    # only 41,000 invoice ids dont match with the total sum of the invoice items
    
    return df

def clean_nexseller_customer(df):

    # Cleans and combines address columns into a single address column
    df = clean_address(df, ['address_1', 'address_2', 'address_3'])
    df = clean_address(df, ['customer_tax_address_1', 'customer_tax_address_2'])

    # Cleans phone number columns
    phone_cols = ['phone', 'contact_phone']
    for col_name in phone_cols:
        df = normalize_phone_number(df, f"{col_name}")
        df = validate_phone_number(df, f"{col_name}")
        df = validate_phone_number(df, f"{col_name}_normalized")
        df = phone_number_status(df, f"{col_name}", f"{col_name}_normalized")

    # Cleans NIK
    df = clean_nik(df, "customer_nik")

    # Cleans coordinates
    df = clean_coordinates(df, 'latitude', 'longitude')

    # Cleans postal code
    df = clean_postal_code(df, 'postal_code')

    # Cleans name columns
    name_cols = ['name', 'contact_name', 'customer_ktp_name', 'customer_tax_name']
    for col_name in name_cols:
        df = clean_name(df, col_name)

    df = clean_string_location(df, 'city')

    df = clean_padded_char(df, ['is_bumn', 'is_pkp', 'is_tax_free'], ['Y', 'N'])

    return df

def clean_nexseller_sales_invoice(df):

    # Join with geo tree nd mapping nexseller to get the geo tree code hierarchy for the distributor making the sales invoice.
    # we dont need that much granularity in location for area product performance, so geo tree code should suffice.

    df = clean_padded_char(df, ['selling_type'], ['TO', 'CV'])
    df = df.withColumn(
            'status',
            initcap(col('status'))
        ).withColumn(
            'status',
            when(col('status') == 'Proccess', 'Process').otherwise(col('status'))
        )
    df = clean_padded_char(df, ['status'], ['Done', 'Process'])

    return df

def clean_nexseller_product(df):

    # fill product ids after joining np and p on product code, along with packaging (?), name, description, uom 1 to 4. conversions.

    # fix the discontinued items.
    # remove distributor items, aka keep only items that have valid product codes.

    df = df.withColumn(
                    "status",
                    when((col("status") != "A") & (col("status") != "D"), lit(None))
                    .otherwise(col("status"))
                )
    return df

def clean_product(df):

    #Corrects the free good items and promo items.
    df = df.withColumn(
        "is_free_good_item",
        when(col("principal_product_code").contains("FG"), "Y").otherwise("N")
    ).withColumn(
        "is_promo_item",
        when(col("principal_product_code").contains("PRM"), "Y").otherwise("N"))                    

    return df

def clean_SOURCE_geo_tree(df):

    return df
