import datetime
import mysql.connector
import argparse
import csv

def parse_args():
    parser = argparse.ArgumentParser(description = 'Libreria para consulta de precios de productos agriculas FCCA UMICH V.1.1')
    parser.add_argument('product',    help='Producto [fyh: frutas y hortalizas, granos: granos]', choices=['fyh', 'granos'])
    parser.add_argument('start_date', help='Fecha inicio [YYYY-mm-dd]')
    parser.add_argument('end_date',   help='Fecha fin [YYYY-mm-dd]')
    args = parser.parse_args()
    return args

def main():
    inputs = parse_args()
    db_con(inputs)

def db_con(inputs):
    start_date = inputs.start_date
    end_date   = inputs.end_date
    if inputs.product == 'granos':
        get_data('granos', 'sniim_maiz_1', start_date,end_date)
    if inputs.product == 'fyh':
        get_data('fyh', 'sniim_frutas_hortalizas_1', start_date, end_date)

def get_data(file_name, product, start_date, end_date):
    print('REPORTE EJECUTADO')
    print('Fecha Inicial: ' + str(start_date))
    print('Fecha Final: '   + str(end_date))
    mydb = mysql.connector.connect(host="18.215.228.120",user="fcca_read_1",password="fcca_read_1", port=3011, database="fcca_1")
    mycursor = mydb.cursor()
    mycursor.execute("SELECT * FROM "+ str(product) + " WHERE (STR_TO_DATE(fecha, '%d/%m/%Y') BETWEEN '" + str(start_date) + "' AND '" + str(end_date) + "') ")

    f = open('./data/'+str(file_name)+'_'+str(start_date)+'_'+str(end_date), 'w')
    writer = csv.writer(f, delimiter="|")
    writer.writerows(mycursor.fetchall())
    f.close

if __name__ == '__main__':
    main()
