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
import os
import pdb


class Replicate4Kafka(): 
    '''从kafka读取replicate数据，并应用至目标数据库'''

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
                self.db= cx_Oracle.connect(self.targetdb['user'],self.targetdb['passwd'],self.targetdb['tns'])
                self.cursor= self.db.cursor()
            except Exception,e:
                self.logger.error('connect to target database error:%s' % (str(e)))
                sys.exit(1)


    def __init__(self,dtopic='ARDATA',mtopic='ARMSG',
                 kafkaserver='127.0.0.1:9092',groupid='ar-group', logfix='1'):
        #multiprocessing.Process.__init__(self)
        #self.stop_event = multiprocessing.Event()
        self.stopflag = multiprocessing.Value('i',0)

        self.loggerbase = self.__getLog('kafka',
                                        os.path.join(self.__scriptPath,'kafka'+str(logfix)+'.log') ,level=logging.WARNING)
        self.logger = self.__getLog('arforkafka',
                                    os.path.join(self.__scriptPath,'repkafka'+str(logfix)+'.log'),level=logging.INFO)
        self.datalog = self.__getLog('data',
                                     os.path.join(self.__scriptPath,
                                                  'exception'+str(logfix)+'.log'),level=logging.INFO)
        self.datatopic = dtopic
        self.messagetopic = mtopic
        self.server = kafkaserver
        self.groupid = groupid
        self.consumer_timeout = 60
        self.autooffset = 'latest'
        self.__getTargetDB()

    def stop(self):
        self.logger.info('revice stop signal,set stopflag to 1')
        self.stopflag.value = 1
        #self.stop_event.set()

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


    def __makeIsql(self,tb,op,fieldlist):
        '''生成insert sql语句'''
        sql = ""
        flist = ','.join(fieldlist)
        plist = ','.join(':%d'%(n+1) for n in range(len(fieldlist)))
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
        wlist = ' AND '.join("%s=:%d" % (flist[n],n+1) for n in range(len(flist)))
        sql = "delete from %s where %s " % (self.__getTargetTableNm(tb), wlist)
        return [sql,dlist]


    def __post2Target1by1(self,tb,op,fieldlist,data,bdata):
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


    def readDataTopic(self):
        #c = KafkaConsumer(self.datatopic, group_id='ar-group', bootstrap_servers=self.server, auto_offset_reset='earliest')
        #self.autooffset = 'latest'
        c = KafkaConsumer(self.datatopic, group_id=self.groupid,
                          bootstrap_servers=self.server,
                          consumer_timeout_ms=self.consumer_timeout*1000,
                          enable_auto_commit  = False,
                          auto_offset_reset=self.autooffset)
        self.logger.info('start consumer')
        self.logger.debug('timeout is setting:%d' %(self.consumer_timeout))
        #c = KafkaConsumer(self.datatopic, group_id=self.groupid, bootstrap_servers=self.server, enable_auto_commit  = False)
        cnt = 0
        start_time = time.time()
        for m in c:
            if self.stopflag.value <> 0 :
                self.logger.info('recive stop signal')
                break
            #if self.stop_event.is_set():
            #    break
            try:
                #尝试转换为json格式，并判断是否是ＡＲ数据
                v = json.loads(m.value)
            except:
                self.logger.warn('skip data %s' % (m.value))
                #跳过后续逻辑
                continue
            if v['magic'] == 'atMSG' and v['type']=='DT':
                tb,op,flist,data,beforedata = self.splitData(m)
                self.logger.debug('find data %s ,split to %s,%s,%s' % (m.value,tb,op,str(data)))
                self.__post2Target1by1(tb,op,flist,data,beforedata)
                self.logger.debug('post data %s,%s,%s' % (tb,op,str(data)))
                cnt += 1
            if cnt == 1000 or (time.time()-start_time >= 10):
                #超过5000条记录或者超过１０秒
                self.db.commit()
                c.commit();
                self.logger.info('commit post data,count:%d, wait time:%f'%(cnt,time.time()-start_time))
                cnt = 0
                start_time = time.time()
        self.logger.info('quit loop read')
        if cnt > 0:
            self.db.commit()
            c.commit()
        c.close()

    def readDataByBatch(self):
        c = KafkaConsumer(self.datatopic, group_id=self.groupid,
                          bootstrap_servers=self.server,
                          #consumer_timeout_ms=self.consumer_timeout*1000,
                          enable_auto_commit  = False,
                          fetch_max_bytes = 100*1024*1024,
                          max_poll_records = 5000,
                          receive_buffer_bytes  = 32768*4,
                          auto_offset_reset=self.autooffset)
        self.logger.info('start batch consumer')
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
                        self.logger.debug('find data %s ,split to %s,%s,%s' % (m.value,tb,op,str(data)))
                        #self.__post2Target1by1(tb,op,flist,data,beforedata)
                        self.logger.debug('post data %s,%s,%s' % (tb,op,str(data)))
            #self.db.commit()
            c.commit()
            self.logger.info('commit postdata')
        c.close()

    def run(self):
        self.putAllMetaList()
        while self.stopflag.value == 0:
            self.readDataTopic()
            time.sleep(self.consumer_timeout)

    def start_syncByBatch(self):
        self.putAllMetaList()
        self.readDataByBatch()




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

    c.targetdb = {
        "type" : "oracle",
        "tns"  : "192.168.0.108:1521/test",
        "user" : "test",
        "passwd" : "test"
    }
    # 要复制的table
    c.tblist = ['ARTEST','ARTEST2']
    c.consumer_timeout = 5
    c.start_syncByBatch()

    #c.run()
    #p1 = multiprocessing.Process(target=c.start_syncByBatch,args=())
    #p2 = multiprocessing.Process(target=c.start_syncByBatch,args=())
    #p1.daemon = True
    #p2.daemon = True
    #p1.start()
    #p2.start()

    #p1.join()
    #p2.join()



    #p.autooffset = 'earliest'




            #p.stopflag.value = 1


    #for i in range(multiCount):

    #for p in consumers:
    #    # 目标数据库配置
    #    p.targetdb = {
    #        "type" : "oracle",
    #        "tns"  : "192.168.0.108:1521/test",
    #        "user" : "test",
    #        "passwd" : "test"
    #    }
    #    # 要复制的table
    #    p.tblist = ['ARTEST','ARTEST2']
    #    p.consumer_timeout = 5
    #    #p.autooffset = 'earliest'
    #    time.sleep(2)

    #    print 'pid:%s' %(p.pid)



    #st = time.time()
    #while True:
    #    stopnum=0
    #    for p in consumers:
    #        print '--------------------------------------'
    #        print 'active process:%d'%(p.pid),p.is_alive()
    #        if not p.is_alive():
    #            stopnum += 1
    #    if stopnum == len(consumers): break

    #    time.sleep(2)
    #print time.time()-st


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

