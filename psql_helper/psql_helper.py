import psycopg2


class PsqlConnection(object):

    def __init__(self, db, user, host='localhost'):
        self.conn = psycopg2.connect(dbname=db, user=user, host=host)
        self.cur = self.conn.cursor()
        self.db = db
        self.user = user
        self.host = host
        print 'Connection Open'

    def create_table(self, headers, table_name):
        create_query = 'CREATE TABLE {0} ({1});'
        cols = self._varchar_columns(headers)
        self.cur.execute(create_query.format(table_name, cols))
        self.conn.commit()
        print 'Table {0} created in {1}'.format(table_name, self.db)

    def _varchar_columns(self, headers):
        var = '{0} varchar(100)'
        cols = [var.format(header) for header in headers]
        return ', '.join(cols)

    def insert_csv(self, table_name, csv_path, if_header=True):
        copy_query = "COPY {0} FROM '{1}' WITH DELIMITER ',' CSV {2};"
        if if_header:
            header = 'HEADER'
        else:
            header = ''
        self.cur.execute(copy_query.format(table_name, csv_path, header))
        self.conn.commit()
        print 'CSV inserted into {0}.'.format(table_name)

    def drop_table(self, table_name):
        drop_query = 'DROP TABLE IF EXISTS {0};'
        self.cur.execute(drop_query.format(table_name))
        self.conn.commit()
        print 'Table {0} dropped.'.format(table_name)

    def end_connection(self):
        self.conn.close()
        print 'Connection Closed'