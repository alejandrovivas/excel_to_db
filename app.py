from flask import Flask, request
from openpyxl import load_workbook
from openpyxl.drawing.image import Image
import pandas as pd
import pymysql
import logging
from io import BytesIO
import boto3
import re
from datetime import datetime
from categorias import (categorias, HOST, PASSWORD, PORT, USER, DATABASE,AWS_BUCKET_NAME, AWS_DEFAULT_REGION,
                        AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_IMAGES_PATH)

app = Flask(__name__)

# Configure logging
timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
log_filename = f'excel_{timestamp}.log'
logging.basicConfig( filename=log_filename, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s',
                     datefmt='%Y-%m-%d %H:%M:%S')

@app.route('/upload-excel', methods=['POST'])
def upload_excel():
    if 'file' not in request.files:
        return 'No file part in the request', 400

    file = request.files['file']
    if file.filename == '':
        return 'No selected file', 400

    # try:
    # # Read the Excel file
    excel_file = pd.read_excel(file, sheet_name=None)

    # Load the workbook and the specific sheet with openpyxl
    wb = load_workbook(file.stream)

    # We change to False the active status of each product
    execute_sql("UPDATE producto SET activo = 0 WHERE activo = 1;")

    # Iter each excel sheet
    for sheet_name, dataframe in excel_file.items():
        logging.info(f'Cargando datos: {sheet_name}')
        #Validate categories on local dictionary
        category = find_category(sheet_name)
        if category:
            # Get products category from database
            category_db = execute_sql("SELECT id_categoria FROM categoria WHERE nombre=%s", (category,))

            # Check if dictionary is empty
            if not category_db:
                logging.error(f'Error cargando datos de la categoria {sheet_name}, '
                              f'no se encontro la categoria en la base de datos')
                continue
            else:
                ws = wb[sheet_name]
                for index, row in dataframe.iterrows():
                    row, validation = validate_data(row)
                    cell = ws.cell(row=index, column=8)

                    for image in ws._images:
                        if image.anchor == cell.coordinate:
                            save_image_to_s3(image)
                            return

                    raise ValueError(f"No image found in cell '{cell}'.")

                    # if validation:
                    #     # TODO SAVE IMAGES SDFASDFSADASFSDASDDSFASDF.PNG
                    #
                    #
                    #     query = """INSERT INTO producto (codigo_ean, titulo, descripcion, precio, url_imagen,
                    #             id_categoria, activo) VALUES (%s, %s, %s, %s, %s, %s %s)"""
                    #     args = (row['CODIGO EAN'], row['REFERENCIA'], row['DESCRIPCION'],
                    #             row['PRECIO EN ALMACENES DE CADENA'], row['IMAGEN'], category['id_categoria'], True)
                    #     execute_sql(query, args)
                    # else:
                    #     return f'Error uploading data: there is an error with file: {index} on {sheet_name} sheet'
            break
        else:
            logging.error(f'Error cargando datos de la categoria {sheet_name}, no se encontro la categoria en los archivos locales')
            continue
    #
    #     return 'Data uploaded successfully', 200
    # except Exception as e:
    #     return f'Error uploading data: {str(e)}', 500

def find_category(search_value):
    for key, values in categorias.items():
        if isinstance(values, list):
            if search_value in values:
                return key
        else:
            if search_value == values:
                return key
    return None


def save_image_to_s3(image):
    # Convert the image to a byte stream
    img_stream = BytesIO()
    image.image.save(img_stream, format='PNG')
    img_stream.seek(0)

    # Initialize a session using boto3
    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID,
                      aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                      region_name=AWS_DEFAULT_REGION)

    # Upload the image to the specified S3 bucket
    s3.put_object(Bucket=AWS_BUCKET_NAME, Key=AWS_DEFAULT_IMAGES_PATH, Body=img_stream, ContentType='image/png')

    print(f"Image uploaded to S3 bucket '{AWS_BUCKET_NAME}' with key '{AWS_DEFAULT_IMAGES_PATH}'")

def execute_sql(query, args=None):
    # Connect to the database
    connection = pymysql.connect(host=HOST, port=PORT, user=USER, password=PASSWORD,database=DATABASE,
                                 cursorclass=pymysql.cursors.DictCursor)

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
    row['DESCRIPCION'] = row['DESCRIPCION'][:1000] if pd.notna(row['DESCRIPCION']) else None
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
