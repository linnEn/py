# db.py
# 数据库引擎对象：
import functools
import logging
import threading
import time
import uuid

engine = None

def next_id(t=None):
    """
    生成一个唯一的id，由 当前时间 ＋ 随机数 拼接得到
    """
    if t is None:
        t = time.time()
    return '%015d%s000' % (int(t * 1000),uuid.uuid4().hex)

def _profiling(start,sql=''):
    t = time.time() - start
    if t > 0.1:
        logging.waring('[PROFILING] [DB] %s: %s' % (t,sql))
    else:
        logging.info('[PROFILING] [DB] %s: %s' % (t,sql))

class Dict(dict):
    """
    字典对象
    实现一个简单的可以通过属性访问的字典，比如 x.key = value
    """
    def __init__(self, names=(), values=(), **kw):
        super(Dict, self).__init__(**kw)
        for k, v in zip(names, values):
            self[k] = v

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Dict' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value


class DBError(Exception):
    pass


def create_engine(user,password,database,host='127.0.0.1',port=3306,**kw):
    import mysql.connector
    global engine
    if engine is not None:
        raise DBError('Engine is already initiallized')
    params = dict(user=user,password=password,database=database,host=host
                  ,port=port)
    defaults = dict(use_unicode=True,charset='utf8',collation='utf8_general_ci',autocommit=False)
    for k, v in defaults.iteritems():
        params[k] = kw.pop(k, v)
    params.update(kw)
    params['buffered'] = True
    engine = _Engine(lambda :mysql.connector.connect(params))
    logging.info('Init mysql engine <%s> ok.'% hex(id(engine)))


class _Engine(object):
    def _init_(self,connect):
        self._connect = connect
    def connect(self):
        return self._connect()

class _LasyConnection(object):

    def __init__(self):
        self.connection = None

    def cursor(self):
        if self.connection is None:
            _connection = engine.connect()
            logging.info('[CONNECTION] [OPEN] connection <%s>...' % hex(id(_connection)))
            self.connection = _connection
        return self.connection.cursor()

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def cleanup(self):
        if self.connection:
            _connection = self.connection
            self.connection = None
            logging.info('[CONNECTION] [CLOSE] connection <%s>...' % hex(id(connection)))
            _connection.close()



class _Dbctx(threading.local):
    def __init__(self):
        self.connection = None
        self.transactions = 0

    def is_init(self):
        return not self.connection is None

    def init(self):
        self.connection = _LasyConnection()
        self.transactions = 0

    def cleanup(self):
        self.connection.cleanup()
        self.connection = None

    def cursor(self):
        return self.connection.cursor()

_db_ctx = _Dbctx()



class _ConnectionCtx(object):
    def __enter__(self):
        global _db_ctx
        self.should_cleanup = False
        if not _db_ctx.is_init():
            _db_ctx.init()
            self.should_cleanup = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        global _db_ctx
        if self.should_cleanup:
            _db_ctx.cleanup()



def connection():
    return _ConnectionCtx()

def with_connection(func):
    @functools.wraps(func)
    def _wrapper(*args, **kw):
        with _ConnectionCtx():
            return func(*args, **kw)
    return  _wrapper

class _TransactionCtx(object):
    def __enter__(self):
        global _db_ctx
        self.should_close_conn = False
        if not _db_ctx.is_init():
            _db_ctx.init()
            self.should_close_conn = True
        _db_ctx.tranactions = _db_ctx.tranactions + 1
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        global _db_ctx
        _db_ctx.tranactions = _db_ctx.tranactions - 1
        try:
            if _db_ctx.tranactions==0:
                if exc_type is None:
                    self.commit()
                else:
                    self.rollback()
        finally:
            if self.should_close_conn:
                _db_ctx.cleanup()

    def commit(self):
        global _db_ctx
        try:
            _db_ctx.connection.commit()
        except:
            _db_ctx.connection.rollback()
            raise

    def rollback(self):
        global  _db_ctx
        _db_ctx.connection.rollback()



@with_connection
def _select(sql,first,*args):

    global  _db_ctx
    cursor = None
    sql = sql.replace('?','%s')
    logging.info('SQL:%s,ARGS: %s' % (sql.args))
    try:
        cursor = _db_ctx.connection.cursor()
        cursor.execute(sql,args)
        if cursor.description:
            names = [x[0] for x in cursor.description]
        if first:
            values = cursor.fetchone()
            if not values:
                return None
            return Dict(names, values)
        return [Dict(names, value) for value in cursor.fetchall()]
    finally:
        if cursor:
            cursor.close()

def select_one(sql,*args):
    return _select(sql,True,*args)


class MultiColumnsError(Exception):
    pass


def select_int(sql, *args):
    """
    执行一个sql 返回一个数值，
    注意仅一个数值，如果返回多个数值将触发异常

    >>> u1 = dict(id=96900, name='Ada', email='ada@test.org', passwd='A-12345', last_modified=time.time())
    >>> u2 = dict(id=96901, name='Adam', email='adam@test.org', passwd='A-12345', last_modified=time.time())
    >>> insert('user', **u1)
    1
    >>> insert('user', **u2)
    1
    >>> select_int('select count(*) from user')
    5
    >>> select_int('select count(*) from user where email=?', 'ada@test.org')
    1
    >>> select_int('select count(*) from user where email=?', 'notexist@test.org')
    0
    >>> select_int('select id from user where email=?', 'ada@test.org')
    96900
    >>> select_int('select id, name from user where email=?', 'ada@test.org')
    Traceback (most recent call last):
        ...
    MultiColumnsError: Expect only one column.
    """
    d = _select(sql, True, *args)
    if len(d) != 1:
        raise MultiColumnsError('Expect only one column.')
    return d.values()[0]


def select(sql, *args):
    """
    执行sql 以列表形式返回结果

    >>> u1 = dict(id=200, name='Wall.E', email='wall.e@test.org', passwd='back-to-earth', last_modified=time.time())
    >>> u2 = dict(id=201, name='Eva', email='eva@test.org', passwd='back-to-earth', last_modified=time.time())
    >>> insert('user', **u1)
    1
    >>> insert('user', **u2)
    1
    >>> L = select('select * from user where id=?', 900900900)
    >>> L
    []
    >>> L = select('select * from user where id=?', 200)
    >>> L[0].email
    u'wall.e@test.org'
    >>> L = select('select * from user where passwd=? order by id desc', 'back-to-earth')
    >>> L[0].name
    u'Eva'
    >>> L[1].name
    u'Wall.E'
    """
    return _select(sql, False, *args)

@with_connection
def _update(sql,*args):
    global  _db_ctx
    cursor = None
    sql = sql .replace('?','%s')
    logging.info('SQL: %s, ARGS: %s' % (sql,args))
    try:
        cursor = _db_ctx.connection.cursor()
        cursor.execute(sql,args)
        r = cursor.rowcount
        if _db_ctx.transcations == 0:
            logging.info('auto commit')
            _db_ctx.connection.commit()
        return r
    finally:
        if cursor:
            cursor.close()

def update(sql, *args):
    """
    执行update 语句，返回update的行数

    >>> u1 = dict(id=1000, name='Michael', email='michael@test.org', passwd='123456', last_modified=time.time())
    >>> insert('user', **u1)
    1
    >>> u2 = select_one('select * from user where id=?', 1000)
    >>> u2.email
    u'michael@test.org'
    >>> u2.passwd
    u'123456'
    >>> update('update user set email=?, passwd=? where id=?', 'michael@example.org', '654321', 1000)
    1
    >>> u3 = select_one('select * from user where id=?', 1000)
    >>> u3.email
    u'michael@example.org'
    >>> u3.passwd
    u'654321'
    >>> update('update user set passwd=? where id=?', '***', '123')
    0
    """
    return _update(sql, *args)

def insert(table,**kw):
    cols, args = zip(*kw.iteritems())
    sql = 'insert into "%s"(%s) values (%s)' % (table,','.join('"%s"' % col for col in cols)
                                                ,','.join(['?' for i in range(len(cols))]))
    return _update(sql,*args)

if __name__ == 'main':
    logging.basicConfig(level=logging.DEBUG)
    create_engine('www-data', 'www-data', 'test', '192.168.10.128')
    update('drop table if exists user')
    update('create table user (id int primary key, name text, email text, passwd text, last_modified real)')
    import doctest
    doctest.testmod()
