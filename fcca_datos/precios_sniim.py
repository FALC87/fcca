import datetime
import mysql.connector
import logging
import argparse
import csv

def parse_args():
    parser = argparse.ArgumentParser(description = 'Libreria para consulta de precios de productos agriculas FCCA UMICH V.1.1')
    parser.add_argument('product',  help='Producto [fyh: frutas y hortalizas, maiz: maiz blanco y pozolero]', choices=['fyh', 'maiz'])
    parser.add_argument('start_date', type=int, help='Fecha inicio [ddMMAA]')
    parser.add_argument('end_date', type=int, help='Fecha fin [ddMMAA]')
    args = parser.parse_args()
    return args

def main():
    inputs = parse_args()
    db_con(inputs)

def db_con(inputs):
    start_date = inputs.start_date
    end_date   = inputs.end_date
    if inputs.product == 'maiz':
        get_data('maiz', 'sniim_maiz_1', start_date,end_date)
    if inputs.product == 'fyh':
        get_data('fyh', 'sniim_frutas_hortalizas_1', start_date, end_date)

def get_data(file_name, product, start_date, end_date):
    mydb = mysql.connector.connect(host="18.215.228.120",user="fcca_read_1",password="fcca_read_1", port=3011, database="fcca_1")
    mycursor = mydb.cursor()
    mycursor.execute("SELECT * FROM "+str(product))

    f = open('./data/'+str(file_name)+'_'+str(start_date)+'_'+str(end_date), 'w')
    writer = csv.writer(f, delimiter="|")
    writer.writerows(mycursor.fetchall())
    f.close

if __name__ == '__main__':
    main()
