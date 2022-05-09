#!python3

def get_users():
    sql = """
            select * from public.dim_users
    """
    return sql

def get_products():
    sql = """
            select * from public.dim_products
    """
    return sql

def get_location():
    sql = """
            select * from public.dim_location
    """
    return sql

def get_ordersdetails():
    sql = """
            select * from public.fact_orderdetails
    """
    return sql

def list_tables():
    tables = [("user",get_users()),
              ("products",get_products()),
              ("location",get_location()),
              ("orders",get_ordersdetails())]

    return tables

def dwh_fact_orders():
    sql = """
            select 	a."UserID",
                    a."OrderDate",
                    a."Quantity",
                    c."ProductName",
                    c."ProductCategory",
                    c."Price",
                    b."PropertyCity",
                    b."PropertyState",
                    a."Quantity" * c."Price" as "SalesAmount"
            from fact_orderdetails a 
                left join dim_location b on a."PropertyID" = b."Prop ID"
                left join dim_products c on a."ProductID" = c."ProductID" 
    """

    return sql