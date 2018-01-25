# -*- coding: utf-8 -*-
from kafka import KafkaConsumer
import json
import cx_Oracle
from kafka import TopicPartition
import multiprocessing
import time
import datetime
import logging
import logging.handlers
import sys
import signal
import pdb


class Replicate4Kafka():

    # 目标数据库配置
    targetdb = {
        "type" : "oracle",
        "tns"  : "192.168.0.108:1521/test",
        "user" : "test",
        "passwd" : "test"
    }

    # 映射配置
    tablemapping = {
        "prefix" : "K_"
    }

    #null_oracle_str = 'None'
    tblist = ['ARTEST','ARTEST2']
    #metadata dict
    metalist = {}
    # process pool
    ppool = []

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
                self.db= cx_Oracle.connect(self.targetdb['user'],self.targetdb['passwd'],self.targetdb['tns'])
                self.cursor= self.db.cursor()
            except Exception,e:
                self.logger.error('connect to target database error:%s' % (str(e)))
                sys.exit(1)

    def __init__(self,dtopic='ARDATA',mtopic='ARMSG',
                 kafkaserver='127.0.0.1:9092',groupid='ar-group'):
        self.loggerbase = self.__getLog('kafka','/data/project/rkafka/kafka.log',level=logging.WARNING)
        self.logger = self.__getLog('arforkafka','/data/project/rkafka/arforkafka.log')
        self.datalog = self.__getLog('data','/data/project/rkafka/exception.log')
        self.datatopic = dtopic
        self.messagetopic = mtopic
        self.server = kafkaserver
        self.groupid = groupid
        self.stopflag = multiprocessing.Value('i',0)
        self.consumer_timeout = 60
        self.__getTargetDB()

    def __del__(self):
        self.cursor.close()
        self.db.close()
        self.logger.info('task finish')

    def __getTargetTableNm(self, table):
        return self.tablemapping['prefix']+table

    def __putMetaList(self, table, meta):
        self.metalist[table] = meta

    def putAllMetaList(self):
        for t in self.tblist:
            mt = self.getTargetMeta(self.targetdb, self.__getTargetTableNm(t))
            self.__putMetaList(t,mt)

    def getTargetMeta(self, targetdb, table):
        '''get table metadata'''
        """
        tbMeta={
            "col" : [ {"name":"id", "type":"number" } ],
            "pk" : [id]
        }
        """
        tbMeat = {}
        tabdef = []
        if targetdb['type']=='oracle':
            db = cx_Oracle.connect(targetdb['user'],targetdb['passwd'],targetdb['tns'])
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


        #if op in ['INSERT','REFRESH']:
        #    opdt = dt['message']['data']
        #    vv2 = []
        #    for c in meta['col']:
        #        value = None
        #        if c['name'] in opdt.keys():
        #            if opdt[c['name']]:
        #                self.__transDataType(self.targetdb['type'],c['type'],opdt[c['name']] )
        #                #if c['type'] in ['CHAR','VARCHAR','VARCHAR2','NCHAR','NVARCHAR2']:
        #                #    #value = "\'%s\'" % (opdt[c['name']])
        #                #    value = opdt[c['name']]
        #                #elif c['type'] == 'DATE' :
        #                #    #value = "to_date(\'%s\',\'yyyy-mm-dd hh24:mi:ss\')" % (opdt[c['name']])
        #                #    value = datetime.datetime.strptime(opdt[c['name']], '%Y-%m-%d %H:%M:%S') 
        #                #elif c['type'] == 'TIMESTAMP':
        #                #    value = datetime.datetime.strptime(opdt[c['name']], '%Y-%m-%d %H:%M:%S') 
        #                #else:
        #                #    value = opdt[c['name']]
        #        vv2.append(value)
        #    #sql = "insert into %s(%s) values(%s) " % (self.__getTargetTableNm(tbnm), flist ,','.join(vv2))
        #    return tbnm,op,vv2

    def __makeIsql(self,tb,op,fieldlist):
        sql = ""
        flist = ','.join(fieldlist)
        plist = ','.join(':%d'%(n+1) for n in range(len(fieldlist)))
        sql = "insert into %s(%s) values(%s) " % (self.__getTargetTableNm(tb), flist ,plist)
        return sql

    def __makeDsql(self,tb,op, fieldlist,datalist):
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
        wlist = ' AND '.join("%s=:%d" % (flist[n],n+1) for n in range(len(flist)))
        sql = "delete from %s where %s " % (self.__getTargetTableNm(tb), wlist)
        return [sql,dlist]


    def __post2oracle1by1(self,tb,op,fieldlist,data,bdata):
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
                self.cursor.execute(sql[0],sql[1])
            rs = True
            #self.db.commit()
        except Exception, e:
            self.logger.error(str(e))
            self.datalog.error('%s,%s,%s' % (sql[0],str(sql[1]),str(e)))
        return rs

    def __post2oraclebatch(self,tb,op,datas):
        try:
            sql = self.__makesql(tb,op)
            self.cursor.execute(sql,datas)
            self.db.commit()
        except Exception, e:
            self.logger.error(str(e))


    def readDataTopic(self):
        #c = KafkaConsumer(self.datatopic, group_id='ar-group', bootstrap_servers=self.server, auto_offset_reset='earliest')
        c = KafkaConsumer(self.datatopic, group_id=self.groupid,
                          bootstrap_servers=self.server,
                          consumer_timeout_ms=self.consumer_timeout*1000,
                          enable_auto_commit  = False)
        self.logger.info('start consumer')
        self.logger.debug('timeout is setting:%d' %(self.consumer_timeout))
        #self.logger.info('%d' %(self.stopflag.value))
        #c = KafkaConsumer(self.datatopic, group_id=self.groupid, bootstrap_servers=self.server, enable_auto_commit  = False)
        """
        rows=[]
        oldtb=''
        oldop=''
        tb =''
        op =''
        newbatch=True
        """
        cnt = 0
        for m in c:
            if self.stopflag.value <> 0 : break
            try:
                #尝试转换为json格式，并判断是否是ＡＲ数据
                v = json.loads(m.value)
            except:
                self.logger.warn('skip data %s' % (m.value))
                continue
            if v['magic'] == 'atMSG' and v['type']=='DT':
                tb,op,flist,data,beforedata = self.splitData(m)
                self.logger.debug('find data %s ,split to %s,%s,%s' % (m.value,tb,op,str(data)))
                self.__post2oracle1by1(tb,op,flist,data,beforedata)
                self.logger.debug('post data %s,%s,%s' % (tb,op,str(data)))
                cnt += 1
            if cnt == 5000:
                self.db.commit()
                c.commit();
                cnt = 0
        if cnt > 0:
            self.db.commit()
            c.commit()
        c.close()
            #if (tb==oldtb) and (op==oldop):
            #    #同一张表的相同操作,合并
            #    newbatch = False
            #    rows.append(data)
            #else:
            #    newbatch = True
            #    #如果是新的batch
            #    if rows:
            #        #插入数据到目标
            #        #print oldtb,oldop,rows
            #        self.__post2oraclebatch(oldtb,oldop,rows)
            #    rows=[]
            #    rows.append(data)
            #    oldtb = tb
            #    oldop = op

    #def initProcessPool(self, processcount=1):
    #    self.logger.info('create multiprocess object %d' % (processcount))
    #    for i in range(processcount):
    #        p = multiprocessing.Process(target = self.readDataTopic, args = ())
    #        self.ppool.append(p)
    #        p.daemon = True

    def newSyncProcess(self):
        #self.putAllMetaList()
        """
        pool = multiprocessing.Pool(processes = processcount)
        while self.stopflag.value == 0:
            #pool.apply_async(self.readDataTopic)
            pool.apply(self.readDataTopic,())
            time.sleep(30)
        pool.close()
        self.logger.debug('close process pool,wait all process finsih')
        pool.join()
        self.logger.debug('all process finsih')
        """

        p = multiprocessing.Process(target = self.readDataTopic, args = ())
        self.ppool.append(p)
        p.daemon = True
        p.start()
        self.logger.info('Process %d ,%s is ready'%(p.pid,p.name))

    def stop(self):
        self.logger.info('revice stop signal,set stopflag to 1')
        self.stopflag.value = 1

    def getchstatus(self):
        for p in self.ppool:
            print p.pid,p.name,p.is_alive()


if __name__ == "__main__":
    #init object
    r4k = Replicate4Kafka(dtopic='ARDATA',kafkaserver='192.168.0.122:9092',
                          groupid='ar-group')
    signal.signal(signal.SIGINT,r4k.stop)
    #signal.signal(signal.SIGQUIT,sigHandler)
    #signal.signal(signal.SIGTERM,sigHandler)
    # 目标数据库配置
    r4k.targetdb = {
        "type" : "oracle",
        "tns"  : "192.168.0.108:1521/test",
        "user" : "test",
        "passwd" : "test"
    }
    # 要复制的table
    r4k.tblist = ['ARTEST','ARTEST2']
    r4k.consumer_timeout = 5
    #get metadata info
    r4k.putAllMetaList()
    #print r4k.metalist
    r4k.readDataTopic()
    #r4k.newSyncProcess()
    #while True:
    #    if r4k.stopflag.value <> 0 :
    #        for p in r4k.ppool:
    #            if p.is_alive():
    #                p.join()
    #    time.sleep(2)


    #while True:
    #    print "----------------------------------------------------------"
    #    print "input q to Exit\n      l to List sche\n      c to get All Task Run Time\n      Enter to Next"
    #    inp=raw_input()
    #    if inp=='q':
    #        r4k.stop()
    #        break
    #    elif inp=='l':
    #        r4k.getchstatus()
    #    elif inp=='c':
    #        pass

