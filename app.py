from flask import Flask, request
from openpyxl import load_workbook
from openpyxl.drawing.image import Image
import pandas as pd
import pymysql
import logging
import os
# import io
# import boto3
# from botocore.exceptions import NoCredentialsError
import re
from categorias import categorias

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
        # Read the Excel file
        excel_file = pd.read_excel(file, sheet_name=None)
        # use openpyxl to get images data
        temp_file_path = os.path.join('/tmp', file.filename)
        file.save(temp_file_path)
        wb = load_workbook(temp_file_path)

        # We change to False the active status of each product
        execute_sql("UPDATE producto SET activo = 0 WHERE activo = 1;")

        # Iter each excel sheet
        for sheet_name, dataframe in excel_file.items():
            #TODO validar categoria en archivo de categorias
            logger.info(sheet_name)
            category = category_search(sheet_name)

            #Get products category
            category = execute_sql("SELECT id_categoria FROM categoria WHERE nombre=%s", (sheet_name,))

            if not category:  # Check if dictionary is empty
                return f'Error uploading data: there is not category {sheet_name}'
            else:
                pass
                # ws = wb[sheet_name]
                # for index, row in dataframe.iterrows():
                #     row, validation = validate_data(row)
                #     if validation:
                #         # TODO SAVE IMAGES SDFASDFSADASFSDASDDSFASDF.PNG
                #         #Verify if there is an image on row
                #         for image in ws._images:
                #             if image.anchor._from.row == index and image.anchor._from.col == row['Imagen']:
                #
                #                 # Nombre del archivo de salida
                #                 image_filename = os.path.join(output_folder, f"imagen_{index}.png")
                #
                #                 # Guarda la imagen
                #                 image_bytes = image._data
                #                 image_file = io.BytesIO(image_bytes)
                #                 with open(image_filename, 'wb') as f:
                #                     f.write(image_file.read())
                #
                #                 print(f"Imagen guardada en: {image_filename}")
                #
                #
                #         cell_coordinate = row['Imagen']
                #
                #         query = """INSERT INTO producto (codigo_ean, titulo, descripcion, precio, url_imagen, id_categoria, activo)
                #                 VALUES (%s, %s, %s, %s, %s, %s %s)"""
                #         args = (row['CODIGO EAN'], row['REFERENCIA'], row['DESCRIPCION'], row['PRECIO EN ALMACENES DE CADENA'], row['IMAGEN'], category['id_categoria'], True)
                #         execute_sql(query, args)
                #     else:
                #         return f'Error uploading data: there is an error with file: {index} on {sheet_name} sheet'
            break

        return 'Data uploaded successfully', 200
    except Exception as e:
        return f'Error uploading data: {str(e)}', 500

def category_search(sheet_name):
    for key, values in categorias.items():
        if sheet_name == values:
            return key
    return None  # return None if there is not any category

# def upload_to_s3(file_name, bucket, object_name):
#     # Crear una sesión de S3
#     s3_client = boto3.client('s3')
#
#     try:
#         # Subir el archivo
#         s3_client.upload_file(file_name, bucket, object_name)
#         print(f"Archivo {file_name} subido exitosamente a {bucket}/{object_name}")
#     except FileNotFoundError:
#         print("El archivo no fue encontrado")
#     except NoCredentialsError:
#         print("Credenciales no disponibles")
#     except Exception as e:
#         print(f"Error al subir el archivo: {e}")


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
