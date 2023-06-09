import datetime
import requests
from bs4 import BeautifulSoup
from clint.textui import puts, colored, indent
from db.mongo import Mongoclient
from db.mysql import Mysqlclient

class ScrapperMarketAgriculture:
    total_records = 0
    inserted_records = 0

    base_url =  'http://www.economia-sniim.gob.mx/nuevo/Consultas/MercadosNacionales/PreciosDeMercado/Agricolas/' 
    init_urls = [['Maiz', '/ConsultaGranos.aspx?SubOpcion=6', '/ResultadosConsultaFechaGranos.aspx'],]

    def __init__(self, *args, **kwargs):
        self.is_historic = False
        self.mongo = Mongoclient(db_collection='maiz')
        self.mysql = Mysqlclient(db_table='sniim_maiz_1', db='fcca_1')

    def read_category(self, category, url, url_form):
        category_page = requests.get(self.base_url + url)
        category_page = BeautifulSoup(category_page.content, features="html.parser")

        products = [(product.getText(), product['value'], ) for product in category_page.select_one('select#ddlProducto').find_all('option')]

        for product in products:
            product_name, product_id = product
            if product_id == '-1':
                continue
            #----------------------------------------------------------------------
            # fcca: para restringir los productos que se van obtener
            if 'Maíz' not in product_name:
                continue
            #----------------------------------------------------------------------

            with indent(4):
                puts(colored.magenta("Producto: {}".format(str(product_name))))

            today = datetime.datetime.today()
            deleta = datetime.timedelta(days=-1)
            payload = {
                    'RegistrosPorPagina':'1000',
                    'Semana':'2',
                    'Mes':'7',
                    'Anio':'2022',
                    'ProductoId':product_id,
                    'OrigenId':'-1',
                    'DestinoId':'-1',
                }

            if not self.gather_prices(payload, url_form, product_name):
                continue

        return

    def scraping(self):
        self.total_records = 0
        self.inserted_records = 0

        for category, url, url_form in self.init_urls:
            self.read_category(category, url, url_form)

    def gather_prices(self, payload, url_form, product_name):
        with indent(4):
            puts(colored.blue("Peticion: {}".format(str(payload))))

        response = requests.get(self.base_url + url_form, params=payload)

        if response.status_code != 200:
            with indent(4):
                puts(colored.red("Error en la peticion HTTP: {}".format(str(response.text))))
            return False

        product_prices = BeautifulSoup(response.content, features="html.parser")

        try:
            table_prices = product_prices.select_one('table#tblResultados')
        except Exception as error:
            with indent(4):
                puts(colored.red("Error en el parseo: {}".format(str(error))))
            return False

        fields = ('producto', 'fecha', 'origen', 'destino', 'precio_min', 'precio_max', 'precio_frec', 'obs')
        counter_row = 0

        #print(table_prices)
        for observation in table_prices.find_all('tr'):
            if counter_row > 1:
                row = {}

                counter_field = 0
                row[fields[counter_field]] = str(product_name)
                counter_field = 1

                for metric in observation.find_all('td'):
                    #puts(colored.yellow("FCCA Respuesta: {}".format(str(metric))))
                    row[fields[counter_field]] = metric.getText()
                    #print(metric.getText())
                    counter_field += 1

                with indent(4):
                    puts(colored.yellow("Insertando: {}".format(str(row))))

                if self.mongo.insert_one(row):
                    self.inserted_records += 1
                    with indent(4):
                        puts(colored.green("Insertado: {}".format(str(row))))
                else:
                    with indent(4):
                        puts(colored.red("No Insertado: {}".format(str(row))))

                #------------------------------
                # Ingresar datos a MySQL
                #------------------------------
                mysql_row = (row['producto'],row['fecha'],row['origen'],row['destino'],row['precio_min'],row['precio_max'],row['precio_frec'],row['obs'])
                if self.mysql.insert_maiz(mysql_row):
                    #self.inserted_records += 1
                    with indent(4):
                        puts(colored.green("Insertado MySQL: {}".format(str(mysql_row))))
                else:
                    with indent(4):
                        puts(colored.red("No Insertado MySQL: {}".format(str(mysql_row))))


            self.total_records += 1
            counter_row += 1

        return True

if __name__ == '__main__':
    maiz = ScrapperMarketAgriculture()
    maiz.scraping()

