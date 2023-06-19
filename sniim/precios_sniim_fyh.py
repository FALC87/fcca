import datetime
#from datetime import date
from dateutil.rrule import rrule, DAILY
import requests
from bs4 import BeautifulSoup
from clint.textui import puts, colored, indent
from db.mongo import Mongoclient
from db.mysql import Mysqlclient
import logging

logging.basicConfig(filename='./logs/precios_sniim_fyh.log', level=logging.ERROR,format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger=logging.getLogger(__name__)
today = datetime.datetime.today()
delta = datetime.timedelta(days=3)
delta_date = today - delta

#--------------------------------------------------------------------
# Si requiere carga historica especificar las siguientes variables
start_date = datetime.date(2023, 5, 2)
end_date   = datetime.date(2023, 5, 3)
historical_records = False

# Si se requiere restringir la busqueda a ciertos productos
product_list = ['Acelga', 'Jitomate']
product_all = True
#--------------------------------------------------------------------


class ScrapperMarketAgriculture:
    total_records = 0
    inserted_records = 0

    base_url =  'http://www.economia-sniim.gob.mx/nuevo/Consultas/MercadosNacionales/PreciosDeMercado/Agricolas/' 
    init_urls = [['Frutas y Hortalizas', 'ConsultaFrutasYHortalizas.aspx?SubOpcion=4', 'ResultadosConsultaFechaFrutasYHortalizas.aspx'],]

    def __init__(self, *args, **kwargs):
        self.is_historic = False
        self.mysql = Mysqlclient(db_table='sniim_frutas_hortalizas_1', db='fcca_1')

    def read_category(self, category, url, url_form, delta_date):
        category_page = requests.get(self.base_url + url)
        category_page = BeautifulSoup(category_page.content, features="html.parser")

        products = [(product.getText(), product['value'], ) for product in category_page.select_one('select#ddlProducto').find_all('option')]

        for product in products:
            product_name, product_id = product
            if product_id == '-1':
                continue
            if not product_all:
                if product_name not in product_list:
                    continue

            with indent(4):
                puts(colored.magenta("Producto: {}".format(str(product_name))))

            payload = {
                    'RegistrosPorPagina':'1000',
                    'fechaInicio':'{}'.format(delta_date.strftime('%d/%m/%Y')),
                    'fechaFinal': '{}'.format(delta_date.strftime('%d/%m/%Y')),
                    'ProductoId':product_id,
                    'OrigenId':'-1',
                    'Origen': 'Todos',
                    'DestinoId': '-1',
                    'Destino': 'Todos',
                    'PreciosPorId' : '1',
                }

            if not self.gather_prices(payload, url_form, product_name, delta_date):
                continue
        return


    def scraping(self):
        self.total_records = 0
        self.inserted_records = 0

        if historical_records:
            for category, url, url_form in self.init_urls:
                for dt in rrule(DAILY, dtstart=a, until=b):
                    self.read_category(category, url, url_form, delta_date=dt) 
        else:
            for category, url, url_form in self.init_urls:
                self.read_category(category, url, url_form, delta_date=delta_date)


    def gather_prices(self, payload, url_form, product_name, delta_date):
        with indent(4):
            puts(colored.blue("Peticion: {}".format(str(payload))))
            #logger.info("Peticion: {}".format(str(payload)))

        response = requests.get(self.base_url + url_form, params=payload)
        #logger.info('Respuesta HTTP: ' + str(response.text))

        if response.status_code != 200:
            with indent(4):
                puts(colored.red("Error en la peticion HTTP: {}".format(str(response.text))))
                #logger.info("Error en la peticion HTTP: {}".format(str(response.text)))
            return False

        product_prices = BeautifulSoup(response.content, features="html.parser")

        try:
            table_prices = product_prices.select_one('table#tblResultados')
        except Exception as error:
            with indent(4):
                puts(colored.red("Error en el parseo: {}".format(str(error))))
                #logger.info("Error en el parseo: {}".format(str(error)))
            return False

        fields = ('producto', 'fecha', 'presentacion', 'origen', 'destino', 'precio_min', 'precio_max', 'precio_frec', 'obs')
        counter_row = 0

        for observation in table_prices.find_all('tr'):
            if counter_row > 1:
                row = {}

                counter_field = 0
                row[fields[counter_field]] = str(product_name)
                counter_field = 1
                row[fields[counter_field]] = str('{}'.format(delta_date.strftime('%d/%m/%Y')))
                counter_field = 2

                for metric in observation.find_all('td'):
                    row[fields[counter_field]] = metric.getText()
                    counter_field += 1

                #------------------------------
                # Ingresar datos a Mongo
                #------------------------------
                '''
                with indent(4):
                    puts(colored.yellow("Insertando: {}".format(str(row))))
                if self.mongo.insert_one(row):
                    self.inserted_records += 1
                    with indent(4):
                        puts(colored.green("Insertado: {}".format(str(row))))
                else:
                    with indent(4):
                        puts(colored.red("No Insertado: {}".format(str(row))))
                '''

                #------------------------------
                # Ingresar datos a MySQL
                #------------------------------
                mysql_row = (row['producto'],row['fecha'],row['presentacion'],row['origen'],row['destino'],row['precio_min'],row['precio_max'],row['precio_frec'],row['obs'])
                if self.mysql.insert_frutas_hortalizas(mysql_row):
                    with indent(4):
                        puts(colored.green("Insertado MySQL: {}".format(str(mysql_row))))
                else:
                    with indent(4):
                        puts(colored.red("No Insertado MySQL: {}".format(str(mysql_row))))

            self.total_records += 1
            counter_row += 1

        return True

if __name__ == '__main__':
    fyh = ScrapperMarketAgriculture()
    fyh.scraping()

