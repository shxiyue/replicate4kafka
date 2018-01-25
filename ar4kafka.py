# -*- coding: utf-8 -*-
from kafka import KafkaConsumer
import json
import cx_Oracle
import MySQLdb
#from kafka import TopicPartition
import multiprocessing
import time
import datetime
import logging
import logging.handlers
import sys
import signal
import os
import pdb
import threading


class Replicate4Kafka():
    '''从kafka读取replicate数据，并应用至目标数据库'''

    # 目标数据库配置
    targetdb = {
        "type" : "oracle",
        "tns"  : "192.168.0.108:1521/test",
        "user" : "test",
        "passwd" : "test"
    }

    targetdb = {
        "type" : "mysql",
        "ip"  : "192.168.0.108",
        "user" : "root",
        "passwd" : "Xy123456*",
        "database":"testdb"
    }


    # 映射配置
    tablemapping = {
        "prefix" : "K_"
    }

    tblist = ['ARTEST','ARTEST2']
    #metadata dict
    metalist = {}

    __scriptPath = os.path.split(os.path.realpath(__file__))[0]

    def __getLog(self,log, logname, level=logging.DEBUG):
        """INIT LOG SETTING,初始化日志设置"""
        # create logger
        baselog = logging.getLogger(log)
        # Set default log level
        baselog.setLevel(level)
        ch = logging.FileHandler(logname)
        # create formatter
        formatter = logging.Formatter('%(asctime)s [%(levelname)s] [%(name)s] [line:%(lineno)d] %(funcName) s.%(message) s')
        # add formatter to ch
        ch.setFormatter(formatter)
        # add ch to logger
        # The final log level is the higher one between the default and the one in handler
        baselog.addHandler(ch)
        return baselog


    def __getTargetDB(self):
        if self.targetdb['type']=='oracle':
            try:
                db= cx_Oracle.connect(self.targetdb['user'],self.targetdb['passwd'],self.targetdb['tns'])
            except Exception,e:
                self.logger.error('connect to target database error:%s' % (str(e)))
                sys.exit(1)
        elif self.targetdb['type']=='mysql':
            try:
                db = MySQLdb.connect(self.targetdb['ip'],self.targetdb['user'],
                                     self.targetdb['passwd'],
                                     self.targetdb['database'])
            except Exception,e:
                self.logger.error('connect to target database error:%s' % (str(e)))
                sys.exit(1)

        return db


    def __init__(self,dtopic='ARDATA',mtopic='ARMSG',
                 kafkaserver='127.0.0.1:9092',groupid='ar-group', logfix='1'):
        #multiprocessing.Process.__init__(self)
        #self.stop_event = multiprocessing.Event()
        self.stopflag = multiprocessing.Value('i',0)

        self.loggerbase = self.__getLog('kafka',
                                        os.path.join(self.__scriptPath,'kafka'+str(logfix)+'.log') ,
                                        level=logging.WARNING)
        self.logger = self.__getLog('arforkafka',
                                    os.path.join(self.__scriptPath,'repkafka'+str(logfix)+'.log'),
                                    level=logging.DEBUG)
        self.datalog = self.__getLog('data', os.path.join(self.__scriptPath,
                                     'exception'+str(logfix)+'.log'),
                                     level=logging.INFO)
        self.datatopic = dtopic
        self.messagetopic = mtopic
        self.server = kafkaserver
        self.groupid = groupid
        self.consumer_timeout = 60
        self.autooffset = 'latest'

    def stop(self):
        self.logger.info('revice stop signal,set stopflag to 1')
        self.stopflag.value = 1
        #self.stop_event.set()

    def __del__(self):
        self.logger.info('task finish')

    def __getTargetTableNm(self, table):
        '''根据转换规则获取目标表名'''
        return self.tablemapping['prefix']+table


    def __putMetaList(self, table, meta):
        self.metalist[table] = meta

    def putAllMetaList(self):
        for t in self.tblist:
            mt = self.getTargetMeta(self.__getTargetTableNm(t))
            self.__putMetaList(t,mt)

    def getTargetMeta(self, table):
        '''get table metadata'''
        """
        tbMeta={
            "col" : [ {"name":"id", "type":"number" } ],
            "pk" : [id]
        }
        """
        tbMeat = {}
        tabdef = []
        if self.targetdb['type']=='oracle':
            db = cx_Oracle.connect(self.targetdb['user'],self.targetdb['passwd'],self.targetdb['tns'])
            cur = db.cursor()
            sql = '''
                  SELECT a.COLUMN_NAME,a.DATA_TYPE,a.DATA_LENGTH,a.DATA_PRECISION FROM ALL_TAB_COLUMNS A
                  WHERE A.TABLE_NAME='%s'
                  ORDER BY a.COLUMN_ID
                  ''' % (table)
            try:
                cur.execute(sql)
            except Exception, e:
                self.logger.error(str(e))
                sys.exit(1)
            for t in cur.fetchall():
                tabdef.append({'name':t[0],'type':t[1]})
            tbMeat['col'] = tabdef
            sql = '''select column_name ,AU.constraint_name,AU.constraint_type
                  from user_cons_columns cu, user_constraints au
                  where cu.constraint_name = au.constraint_name  and au.table_name = '%s'
                  and au.constraint_type = 'P'
                  ''' % (table)
            try:
                cur.execute(sql)
            except Exception,e:
                self.logger.error(str(e))
                sys.exit(1)
            pkclm = []
            for t in cur.fetchall():
                pkclm.append(t[0])
            tbMeat['pk'] = pkclm
            cur.close()
            db.close()
        elif self.targetdb['type']=='mysql':
            db = self.__getTargetDB()
            cur = db.cursor()
            sql = '''
                  select  column_name,DATA_TYPE from information_schema.columns
                  where table_schema ='%s'  and table_name = '%s'
                  order by ORDINAL_POSITION
                  ''' % (self.targetdb['database'], table)
            try:
                cur.execute(sql)
            except Exception, e:
                self.logger.error(str(e))
                sys.exit(1)
            for t in cur.fetchall():
                tabdef.append({'name':t[0],'type':t[1]})
            tbMeat['col'] = tabdef
            sql = '''select column_name from INFORMATION_SCHEMA.KEY_COLUMN_USAGE where table_schema
                  ='%s'  and table_name = '%s' and constraint_name='PRIMARY'
                  ORDER BY ORDINAL_POSITION ''' % (self.targetdb['database'],
                                                   table)
            try:
                cur.execute(sql)
            except Exception,e:
                self.logger.error(str(e))
                sys.exit(1)
            pkclm = []
            for t in cur.fetchall():
                pkclm.append(t[0])
            tbMeat['pk'] = pkclm
            cur.close()
            db.close()

        return tbMeat

    def __transDataType(self,dbtype,coltype,data):
        if dbtype == 'oracle':
            if coltype in ['CHAR','VARCHAR','VARCHAR2','NCHAR','NVARCHAR2']:
                value = data
            elif coltype == 'DATE' :
                value = datetime.datetime.strptime(data, '%Y-%m-%d %H:%M:%S') 
            elif coltype == 'TIMESTAMP':
                value = datetime.datetime.strptime(data, '%Y-%m-%d %H:%M:%S') 
            else:
                value = data
        elif dbtype == 'mysql':
            if coltype.upper() in ['CHAR','VARCHAR','VARCHAR2','NCHAR','NVARCHAR2']:
                value = data
            elif coltype.upper() == 'DATETIME' :
                value = datetime.datetime.strptime(data, '%Y-%m-%d %H:%M:%S') 
            elif coltype == 'TIMESTAMP':
                value = datetime.datetime.strptime(data, '%Y-%m-%d %H:%M:%S') 
            else:
                value = data
        return value

    def splitData(self, d):
        #获取表名
        #pdb.set_trace()

        key = d.key
        tbnm = key.split('+')[1]
        #获取元数据
        meta = self.metalist[tbnm]

        dt = json.loads(d.value)
        #操作
        op = dt['message']['headers']['operation']
        #基础数据
        opdt = dt['message']['data']
        opbeforedt = dt['message']['beforeData']
        flist = []
        dlist = []
        blist = []
        for c in meta['col']:
            #value = None
            if c['name'] in opdt.keys():
                if opdt[c['name']]:
                    #更新字段列表
                    flist.append(c['name'])
                    #跟新数据列表
                    dlist.append(self.__transDataType(self.targetdb['type'],c['type'],opdt[c['name']]))
                    if opbeforedt:
                        #如果有befordata
                        blist.append(self.__transDataType(self.targetdb['type'],c['type'],opbeforedt[c['name']]))
        return tbnm,op,flist,dlist,blist


    def __makeIsql(self,tb,op,fieldlist):
        '''生成insert sql语句'''
        sql = ""
        flist = ','.join(fieldlist)

        if self.targetdb['type']=='oracle':
            plist = ','.join(':%d'%(n+1) for n in range(len(fieldlist)))
        elif self.targetdb['type'] == 'mysql':
            plist = ','.join('%s' for n in fieldlist)
        sql = "insert into %s(%s) values(%s) " % (self.__getTargetTableNm(tb), flist ,plist)
        return sql

    def __makeDsql(self,tb,op, fieldlist,datalist):
        '''生成DELETE sql语句'''
        meta = self.metalist[tb]
        if meta['pk']:
            flist = []
            dlist = []
            cnt = 0
            for f in fieldlist:
                if f in meta['pk']:
                    flist.append(f)
                    dlist.append(datalist[cnt])
                cnt += 1
        else:
            flist = fieldlist[:]
            dlist = datalist[:]

        sql = ""
        if self.targetdb['type']=='oracle':
            wlist = ' AND '.join("%s=:%d" % (flist[n],n+1) for n in range(len(flist)))
        elif self.targetdb['type']=='mysql':
            wlist = ' AND '.join("%s=%s" % (flist[n],'%s') for n in range(len(flist)))
        sql = "delete from %s where %s " % (self.__getTargetTableNm(tb), wlist)
        return [sql,dlist]


    def __post2Target1by1(self,cursor,tb,op,fieldlist,data,bdata):
        rs = False
        sqlarray =[]
        if op in ['INSERT','REFRESH']:
            sqlarray.append( [self.__makeIsql(tb,op,fieldlist),data])
        elif op =='DELETE':
            sqlarray.append( self.__makeDsql(tb,op,fieldlist,data))
        elif op =='UPDATE':
            sqlarray.append( self.__makeDsql(tb,op,fieldlist,bdata))
            sqlarray.append( [self.__makeIsql(tb,op,fieldlist),data])

        try:
            for sql in sqlarray:
                self.logger.debug('%s,%s'%(sql[0], sql[1]))
                cursor.execute(sql[0],sql[1])
            rs = True
            #self.db.commit()
        except Exception, e:
            self.logger.error(str(e))
            self.datalog.error('%s,%s,%s' % (sql[0],str(sql[1]),str(e)))
        return rs


    def readDataByBatch(self, pnum):
        db= self.__getTargetDB()
        cursor = db.cursor()
        c = KafkaConsumer(self.datatopic, group_id=self.groupid,
                          bootstrap_servers=self.server,
                          #consumer_timeout_ms=self.consumer_timeout*1000,
                          enable_auto_commit  = False,
                          fetch_max_bytes = 100*1024*1024,
                          max_poll_records = 5000,
                          receive_buffer_bytes  = 32768*4,
                          auto_offset_reset=self.autooffset)
        self.logger.info('start batch consumer with pnum:%d' %(pnum))
        self.logger.debug('timeout is setting:%d' %(self.consumer_timeout))
        while self.stopflag.value == 0:
            r = c.poll(timeout_ms=self.consumer_timeout*1000,max_records=10000)
            for x,message in r.items():
                for m in message:
                    try:
                        #尝试转换为json格式，并判断是否是ＡＲ数据
                        v = json.loads(m.value)
                    except:
                        self.logger.warn('skip data %s' % (m.value))
                        #跳过后续逻辑
                        continue
                    if v['magic'] == 'atMSG' and v['type']=='DT':
                        tb,op,flist,data,beforedata = self.splitData(m)
                        self.logger.debug('consumer %d find data %s ,split to %s,%s,%s' 
                                          % (pnum, m.value,tb,op,str(data)))
                        self.__post2Target1by1(cursor,tb,op,flist,data,beforedata)
                        self.logger.info('consumer %d post data %s,%s,%s' %
                                          (pnum,tb,op,str(data)))
                        self.transRecordCount[pnum] += 1
            db.commit()
            c.commit()
            self.logger.info('consumer %d commit postdata' % (pnum))
        cursor.close()
        db.close()
        c.close()

    def __getPartitions(self):
        c = KafkaConsumer(self.datatopic, group_id=self.groupid,
                          bootstrap_servers=self.server,
                          #consumer_timeout_ms=self.consumer_timeout*1000,
                          enable_auto_commit  = False,
                          auto_offset_reset=self.autooffset)
        p = c.partitions_for_topic(self.datatopic)
        c.close()
        return p

    def run(self):
        self.putAllMetaList()
        self.threadpool = []
        partations = self.__getPartitions()
        self.transRecordCount = [0 for p in partations]
        for i in partations:
            th = threading.Thread(target=self.readDataByBatch,args=(i,),name='consumer%d'%(i))
            th.setDaemon(True)
            th.start()
            self.threadpool.append(th)



if __name__ == "__main__":
    #init object
    c=Replicate4Kafka(dtopic='ARDATA',kafkaserver='192.168.0.122:9092',
                      groupid='ar-group',logfix=str(1))

    def shutdown(sig, frame):
        print 'Signal handler called with signal', sig
        c.stop()
    signal.signal(signal.SIGINT,shutdown)
    #signal.signal(signal.SIGQUIT,sigHandler)
    #signal.signal(signal.SIGTERM,sigHandler)

    #oracle target setting
    c.targetdb = {
        "type" : "oracle",
        "tns"  : "192.168.0.108:1521/test",
        "user" : "test",
        "passwd" : "test"
    }


    # 映射配置
    c.tablemapping = {
        "prefix" : "K_"
    }
    # 要复制的table
    c.tblist = ['ARTEST','ARTEST2']

    #mysql target setting
    c.targetdb = {
        "type" : "mysql",
        "ip"  : "192.168.0.108",
        "user" : "root",
        "passwd" : "Xy123456*",
        "database":"testdb"
    }
    c.consumer_timeout = 5

    #测试非线程使用
    #print c.getTargetMeta('k_artest')
    #c.putAllMetaList()
    #c.transRecordCount = [0 for p in range(1)]
    #c.readDataByBatch(0)

    c.run()
    st = time.time()
    while True:
        stopnum=0
        num = 0
        for p in c.threadpool:
            print p.name, p.is_alive(),c.transRecordCount[num]
            num += 1
            if not p.is_alive():
                stopnum += 1
        print '--------------------------------------'
        if stopnum == 5: break
        time.sleep(2)
    print time.time()-st

