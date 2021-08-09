import common as com
import pymssql


class SQLServer(object):
    def __init__(self, name):
        # 创建连接对象
        self.connect = pymssql.connect(host='localhost', server='DESKTOP-FSE8NQS', port='1433',
                                  database='TEST')  # 服务器名,账户,密码,数据库名
        self.cursor = self.connect.cursor()
        self.table_name = name

    def __save__(self):
        self.connect.commit()

    def get_data(self, path):
        self.title = ''
        self.data = ''
        with open(path, 'r') as fr:
            self.data = fr.readlines()
        self.title = self.data[0]
        self.data = self.data[1:len(self.data)]

    def create_table(self, name):
        self.table_name = name
        # 检查表是否为空
        self.cursor.execute(" \
            IF OBJECT_ID('%s', 'U') IS NOT NULL \
            DROP TABLE %s \
            " % (name, name))
        # 创建表头
        create_command = "CREATE TABLE persons ("
        create_command_end = "PRIMARY KEY(id))"
        test_line = self.data[1].strip().split(',')
        for count, i in enumerate(test_line):
            create_command += '%s %s,' % (self.title.split(',')[count], com.get_type(i))
        create_command += create_command_end
        self.cursor.execute(create_command)
        # 输入数据
        insert_command = 'INSERT INTO %s VALUES (' % name
        for i in range(len(self.data[0].split(','))):
            if i < len(self.data[0].split(',')) - 1:
                insert_command += '%s, '
            else:
                insert_command += '%s)'
        data_list = []
        for i in self.data:
            mid_list = i.strip()
            data_list.append(tuple(mid_list.split(',')))
        self.cursor.executemany(insert_command, data_list)
        # 保存
        self.__save__()

    def command(self, command):
        if 'select' in command or 'SELECT' in command:
            # 查询记录
            self.cursor.execute(command)
            # 获取一条记录
            row = self.cursor.fetchone()
            # 循环打印记录(这里只有一条，所以只打印出一条)
            while row:
                print(row)
                row = self.cursor.fetchone()
        else:
            self.cursor.execute(command)
            self.__save__()

    def __del__(self):
        self.connect.close()


s = SQLServer('persons')
s.get_data('test.csv')
s.create_table('persons')
# 改
command = "update persons set name='John' where name='John Smith'"
s.command(command)
# 查
command = "SELECT * FROM persons WHERE salesrep='%s'" % 'John Doe'
s.command(command)
# 删
command = "delete from persons where id=1"
s.command(command)
# 增
command = "INSERT INTO persons VALUES (4, 'Jay1', 'Jay2')"
s.command(command)
command = "SELECT * FROM persons"
s.command(command)



