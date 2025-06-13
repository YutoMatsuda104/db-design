from psycopg2 import connect, ProgrammingError
from psycopg2.sql import SQL, Identifier, Literal

class DataAccess:

    # Constractor called when this class is used. 
    # It is set for hostname, port, dbname, useranme and password as parameters.
    def __init__(self, hostname, port, dbname, username, password):
        self.dburl = "host=" + hostname + " port=" + str(port) + \
                     " dbname=" + dbname + " user=" + username + \
                     " password=" + password

    # # This method is used to actually issue query sql to database. 
    def execute(self, query, data=None):
        try:
            conn = connect(self.dburl)
            conn.autocommit = False
            cur = conn.cursor()
            print(query.as_string(conn))
            if data is None:
                cur.execute(query)
            else:
                cur.execute(query, data)
            conn.commit()
            return cur.fetchall()
        except ProgrammingError as e:
            print(e.args)
            conn.rollback()
            return None
        finally:
            cur.close()
            conn.close()

    def get_conn(self):
        return connect(self.dburl)

    def get_item_by_id(self, id):
        query = SQL("SELECT * FROM items WHERE {} = {}") \
            .format(Identifier("id"), Literal(id))
        return self.execute(query)

    def save_item(self, item):
        query = SQL("INSERT INTO items ( {}, {} ) VALUES ( {}, {} ) RETURNING id") \
            .format(Identifier("name"), Identifier("price"), Literal(item.name), Literal(item.price))
        item.id = self.execute(query)[0][0]
        return item

    def edit_item(self, item):
        query = SQL("UPDATE items SET {} = {}, {} = {} WHERE id = {}") \
            .format(Identifier("name"), Literal(item.name), Identifier("price"), Literal(item.price), Literal(item.id))
        return self.execute(query)

    def del_item(self, item):
        query = SQL("DELETE FROM items WHERE id = {}") \
            .format(Literal(item.id))
        return self.execute(query)

    def save_item_list(self, item_list):
        conn = self.get_conn()
        conn.autocommit = False
        cur = conn.cursor()
        for item in item_list:
            query = SQL("INSERT INTO items ( {}, {} ) VALUES ( {}, {} ) RETURNING id") \
                .format(Identifier("name"), Identifier("price"), Literal(item.name), Literal(item.price))
            cur.execute(query)
            item.id = cur.fetchall()[0][0]
        conn.commit()
        cur.close()
        conn.close()

    def save_item_list_by_executemany(self, item_list):
        conn = self.get_conn()
        cur = conn.cursor()
        query = SQL("INSERT INTO items ( {}, {} ) VALUES ( %s, %s ) RETURNING id") \
            .format(Identifier("name"), Identifier("price"))
        cur.executemany(query, item_list)
        conn.commit()
        conn.close()

class Item():
    def __init__(self, id=0, name="", price=0):
        self.id = id
        self.name = name
        self.price = price

if __name__ == "__main__":
    da = DataAccess("127.0.0.1", "5432", "flaskdb", "yasuhiro", "dbpasswd")

    item = Item(name="カレー", price=500)
    item = da.save_item(item)
    print(item.id)

    item.price = 1500
    da.edit_item(item)

    item_list = [ Item(name="リンゴ", price=200), Item(name="みかん", price=300), Item(name="バナナ", price=100) ]
    da.save_item_list(item_list)
    for item in item_list:
        print(item.id, item.name, item.price)

    item_list = [ ["さんま", 250], ["イワシ", 200], ["ホッケ", 150] ]
    da.save_item_list_by_executemany(item_list)
