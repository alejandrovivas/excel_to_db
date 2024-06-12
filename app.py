from flask import Flask, request
import pandas as pd
import pymysql
import numpy as np
import random
import logging

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO)  # You can adjust the logging level
logger = logging.getLogger(__name__)

@app.route('/upload-excel', methods=['POST'])
def upload_excel():
    if 'file' not in request.files:
        return 'No file part in the request', 400

    file = request.files['file']
    if file.filename == '':
        return 'No selected file', 400

    try:
        # Read the excel file
        excel_file = pd.read_excel(file, sheet_name=None)

        # Iter each excel sheet
        for sheet_name, dataframe in excel_file.items():
            #Get products category
            category = execute_sql("SELECT id_categoria FROM categoria WHERE nombre=%s", (sheet_name,))

            if not category:  # Check if dictionary is empty
                return f'Error uploading data: there is not category {sheet_name}'
            else:
                for index, row in dataframe.iterrows():
                    row, validation = validate_data(row)
                    if validation:
                        aleatory_number = random.randint(1, 9999999)
                        query = """INSERT INTO producto (id_producto, titulo, descripcion, precio, url_imagen, id_categoria)
                                VALUES (%s, %s, %s, %s, %s, %s)"""
                        args = (aleatory_number, row['REFERENCIA'], row['DESCRIPCION'], row['PRECIO EN ALMACENES DE CADENA'],row['IMAGEN'], category['id_categoria'])
                        execute_sql(query, args)
                    else:
                        return f'Error uploading data: there is an error with file: {index} on {sheet_name} sheet'
            break

        return 'Data uploaded successfully', 200
    except Exception as e:
        return f'Error uploading data: {str(e)}', 500

import re

def execute_sql(query, args=None):
    # Connect to the database
    connection = pymysql.connect(host='44.212.212.72', port=3307, user='root',password='Trabajos100@',
                                 database='creditos',cursorclass=pymysql.cursors.DictCursor)

    with connection.cursor() as cursor:
        # Execute SQL query
        cursor.execute(query, args)
        # Return category data if exists
        data = cursor.fetchall()
        if data:
            for row in data:
                return row

    # Make commit to save changes
    connection.commit()

    #Close database connection
    if connection:
        connection.close()

def validate_data(row):
    # Replace NaN with None
    row['REFERENCIA'] = row['REFERENCIA'][:200] if pd.notna(row['REFERENCIA']) else None
    row['DESCRIPCION'] = row['DESCRIPCION'][:400] if pd.notna(row['DESCRIPCION']) else None
    row['PRECIO EN ALMACENES DE CADENA'] = float(row['PRECIO EN ALMACENES DE CADENA']) if pd.notna(row['PRECIO EN ALMACENES DE CADENA']) else None
    row['IMAGEN'] = row['IMAGEN'][:500] if pd.notna(row['IMAGEN']) else None

    # Validate precio value
    precio_str = str(row['PRECIO EN ALMACENES DE CADENA'])
    # Clean value from non-numeric characters
    precio_str = re.sub(r'[^\d.]', '', precio_str)
    # Turn to float
    row['PRECIO EN ALMACENES DE CADENA'] = float(precio_str)

    return row, True

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8001, debug=True)
