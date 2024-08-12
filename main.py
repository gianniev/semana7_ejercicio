#Creación de tabla en RedSHit
#DROP TABLE IF EXISTS gianni_ev93_coderhouse.ingesta_python;
#CREATE table gianni_ev93_coderhouse.ingesta_python(
#    id INT primary KEY,
#    nombre VARCHAR(100),
#    email VARCHAR(100)
#)

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy  import create_engine
import os 


def main():
    load_dotenv()
    users: list[str] = [{
    "id": 1,
    "nombre": "Garfield",
    "email": "ggrellis0@loc.gov"
    }, {
    "id": 2,
    "nombre": "Felicia",
    "email": "fpellatt1@wordpress.com"
    }, {
    "id": 3,
    "nombre": "Dante",
    "email": "dmcnellis2@lulu.com"
    }, {
    "id": 4,
    "nombre": "Genni",
    "email": "gsavil3@constantcontact.com"
    }, {
    "id": 5,
    "nombre": "Dennison",
    "email": "daspey4@omniture.com"
    }]


    data = pd.DataFrame(users)
    
       
    #username =  os.getenv('REDSHIFT_USERNAME')
    #password = os.getenv('REDSHIFT_PASSWORD')
    #host = os.getenv('REDSHIFT_HOST')
    #port = os.getenv('REDSHIFT_PORT', '5439')
    #dbname = os.getenv('REDSHIFT_DBNAME')
    #schema:str = "gianni_ev93_coderhouse"


  

    #connection_url = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{dbname}"
    conn_string = 'postgresql://gianni_ev93_coderhouse:r9NYpl19Zl@data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com:5439/data-engineer-database'
    engine = create_engine(conn_string)  

        
    with engine.connect() as conn:
        data.to_sql("ingesta_python", conn, schema=schema, if_exists="replace", index= False)


if __name__ == "__main__":
    main()

