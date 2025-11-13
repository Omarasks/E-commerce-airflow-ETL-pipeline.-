from airflow.decorators import dag, task 
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk.bases.sensor import PokeReturnValue
from airflow.providers.postgres.hooks.postgres import PostgresHook


#constants 
POSTGRES_CONN_ID = 'postgres' 
url = "https://api.freeapi.app/api/v1/public/randomproducts?page=1&limit=10"
 
# created a DAG pipeline extract_product_data 
@dag 
def extract_product_data():
    # 1. created a new table called product_table in airflow meta database
    create_product_table = SQLExecuteQueryOperator(
        task_id= "create_product_table", 
        conn_id = POSTGRES_CONN_ID, 
        sql= 
             
            """
            CREATE TABLE IF NOT EXISTS product_table(
                product_id INT PRIMARY KEY, 
                title VARCHAR(255), 
                category VARCHAR(255), 
                price FLOAT
            )
            """
    ) 
    
    
    # 2. create a task "extract_product_data" to fetch data from api using a sensor (PokeReturnValue) 
    @task.sensor(timeout=300, poke_interval=30)
    def extract_product_data() -> PokeReturnValue:
        import requests 
        
        response = requests.get(url)
        
        try: 
            if response.status_code == 200:
                condition = True 
                result = response.json() 
                
                all_products = result.get("data", {}).get("data", []) 
                
                print(f"found {len(all_products)} products item") 
                
                extracted_products = [] 
                
                for i in range(len(all_products)):
                    product = all_products[i] 
                    
                    product_info = {
                        "product_id" : product.get("id"), 
                        "title" : product.get("title"), 
                        "category" : product.get("category"), 
                        "price" : product.get("price") 
                    } 
                    
                    extracted_products.append(product_info) 
                return PokeReturnValue(is_done= True, xcom_value=extracted_products)
             
            else:
                condition = False
                print("API failed to fetch response") 
                return PokeReturnValue(is_done= False, xcom_value= None)  
            
             
        except Exception as e:
            print(f"API failed to fetch a response {response.status_code}: {e}")
            return PokeReturnValue(is_done= condition, xcom_value=None)
            
            
    # 3. copy the data received to a csv format         
    @task
    def csv_format(extracted_products): 
        import csv 
        
        # validate if data was received from the api
        if not extracted_products:
            print("No data received from API")
            return 
        else:
            print(f"found {len(extracted_products)} products")  
            
        try:
           with open("/tmp/catalog.csv", "w", newline='') as f:
               fieldnames = list(extracted_products[0].keys()) 
               
               writer = csv.DictWriter(f, fieldnames=fieldnames)
               writer.writeheader()
               writer.writerows(extracted_products)
               
               print(f"Copied {len(extracted_products)} to catalog.csv")
               return extracted_products
             
        
        except Exception as e:
            print(f"Failed to copy {len(extracted_products)} to csv.: {e}") 
            
             
    
    # 4. insert extracted data into product_table using postgres hook 
    
    @task 
    def load_data(extracted_products): 
        
        # validate check if data was received from the csv file
        if not extracted_products:
            print("failed to load catalog.csv ")
        else:
            print(f"Loading {len(extracted_products)} into database") 
            
        # create a postgres hook instance with PostgresHook provider 
        hook = PostgresHook(conn_id= POSTGRES_CONN_ID) 
        
        for product in extracted_products:
            try:
           
                hook.run(
                """
                INSERT INTO product_table(product_id, title, category, price)
                VALUES(%s, %s, %s, %s) 
                ON CONFLICT (product_id)
                DO UPDATE SET 
                    title = EXCLUDED.title,
                    category = EXCLUDED.category, 
                    price = EXCLUDED.price 
                """, 
                
                parameters = (
                    product['product_id'],
                    product['title'],
                    product['category'], 
                    product['price']
                )
            ) 
                print(f"Successfully inserted {len(extracted_products)} into product_table")
            
            
            
            except Exception as e: 
                print(f"Error inserting {product.get('product_id')} into product_table. {e}") 
            


# 5. Create DAG flow with dependencies 

    create_product_table_task = create_product_table 
    extract_product_data_task = extract_product_data() 
    copy_data_to_csv = csv_format(extract_product_data_task) 
    load_data_task_to_db = load_data(copy_data_to_csv)

    create_product_table_task >> extract_product_data_task >> copy_data_to_csv >> load_data_task_to_db


extract_product_data()
            
   
        
        
        
        
        
        
                
                
                
            
    
    
    
    
    
    

  
  
  