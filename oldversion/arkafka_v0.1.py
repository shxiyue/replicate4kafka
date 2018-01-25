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
import pdb


class Replicate4Kafka():
    # kafka setting
    datatopic = "wudi"
    messagetopic = "armessage"
    server = "192.168.0.122:9092"

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
    stopflag = False

    null_oracle_str = 'None'
    tblist = ['ARTEST','ARTEST2']
    metalist = {}
    ppool = []

    def initLog(self, logname, level=logging.DEBUG):
        """INIT LOG SETTING,初始化日志设置"""
        logformat = '%(asctime)s [%(levelname)s] [line:%(lineno)d] %(funcName) s.%(message) s'
        logging.basicConfig(level=level, format=logformat,
                            datefmt='%Y-%m-%dT%H:%M:%S', filename=logname)
        return logging.getLogger()

    def __getTargetDB():
        self.db= cx_Oracle.connect(self.targetdb['user'],self.targetdb['passwd'],self.targetdb['tns'])
        self.cursor= self.db.cursor()


    def __init__(self):
        self.__getTargetDB()
        self.logger = self.initLog(logname='arforkafka.log')

    def __del__(self):
        self.cursor.close()
        self.db.close()

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
            "col" : [
                {"name":"id",
                 "type":"number"
                }
            ],
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
            cur.execute(sql)
            for t in cur.fetchall():
                tabdef.append({'name':t[0],'type':t[1]})
            tbMeat['col'] = tabdef
            sql = '''select column_name ,AU.constraint_name,AU.constraint_type
                  from user_cons_columns cu, user_constraints au
                  where cu.constraint_name = au.constraint_name  and au.table_name = '%s'
                  and au.constraint_type = 'P'
                  ''' % (table)
            #cur.close()
            cur.execute(sql)
            pkclm = []
            for t in cur.fetchall():
                pkclm.append(t[0])
            tbMeat['pk'] = pkclm
            cur.close()
            db.close()
        return tbMeat

    def __makesql(tb,op):
        meta = self.metalist[tb]
        if op =='INSERT':
            flist = ','.join(n['name'] for n in meta['col'])
            plist = ','.join(':%d'%(n+1) for n in range(len(meta['col'])))
            sql = "insert into %s(%s) values(%s) " % (self.__getTargetTableNm(tb), flist ,plist)
            return sql

    def __post2oracle1by1(tb,op,data):
        try:
            sql = self.__makesql(tb,op)
            self.cursor.execute(sql,data)
            self.db.commit()
        except Exception, e:
            self.logger.error(str(e))

    def splitData(self, d):
        #获取表名
        #pdb.set_trace()
        key = d.key
        tbnm = key.split('+')[1]
        #获取元数据
        meta = self.metalist[tbnm]

        dt = json.loads(d.value)
        op = dt['message']['headers']['operation']

        if op =='INSERT':
            opdt = dt['message']['data']
            vv2 = []
            for c in meta['col']:
                value = None
                if c['name'] in opdt.keys():
                    if opdt[c['name']]:
                        if c['type'] in ['CHAR','VARCHAR','VARCHAR2','NCHAR','NVARCHAR2']:
                            #value = "\'%s\'" % (opdt[c['name']])
                            value = opdt[c['name']]
                        elif c['type'] == 'DATE' :
                            #value = "to_date(\'%s\',\'yyyy-mm-dd hh24:mi:ss\')" % (opdt[c['name']])
                            value = datetime.datetime.strptime(opdt[c['name']], '%Y-%m-%d %H:%M:%S') 
                        elif c['type'] == 'TIMESTAMP':
                            value = datetime.datetime.strptime(opdt[c['name']], '%Y-%m-%d %H:%M:%S') 
                        else:
                            value = opdt[c['name']]
                vv2.append(value)
            #sql = "insert into %s(%s) values(%s) " % (self.__getTargetTableNm(tbnm), flist ,','.join(vv2))
            return tbnm,op,vv2

    def changeSQL(self, d):
        key = d.key
        tbnm = key.split('+')[1]
        meta = self.metalist[tbnm]
        dt = json.loads(d.value)
        op = dt['message']['headers']['operation']
        sql = ""
        if op =='INSERT':
            opdt = dt['message']['data']
            flist = ','.join(n['name'] for n in meta['col'])
            #flist = ','.join(n for n in opdt.iterkeys()) 
            #vlist = ','.join(opdt[n] for n in opdt.iterkeys() )
            #vv = [opdt[n] for n in opdt.iterkeys()]
            vv2 = []
            vstr = ""
            for c in meta['col']:
                value = self.null_oracle_str
                if c['name'] in opdt.keys():
                    if opdt[c['name']]:
                        if c['type'] in ['CHAR','VARCHAR','VARCHAR2','NCHAR','NVARCHAR2']:
                            value = "\'%s\'" % (opdt[c['name']])
                        elif c['type'] == 'DATE' :
                            value = "to_date(\'%s\',\'yyyy-mm-dd hh24:mi:ss\')" % (opdt[c['name']])
                        elif c['type'] == 'TIMESTAMP':
                            value = "to_date(\'%s\',\'yyyy-mm-dd hh24:mi:ss\')" % (opdt[c['name']])
                        else:
                            value = opdt[c['name']]
                vv2.append(value)
            sql = "insert into %s(%s) values(%s) " % (self.__getTargetTableNm(tbnm), flist ,','.join(vv2))
            return sql


    def getoffset(self,topic):
        '''目前未使用'''
        from kafka import SimpleClient
        from kafka.protocol.offset import OffsetRequest, OffsetResetStrategy
        from kafka.common import OffsetRequestPayload
        client = SimpleClient(self.server)
        partitions = client.topic_partitions[topic]
        offset_requests = [OffsetRequestPayload(topic, p, -1, 1) for p in partitions.keys()]
        offsets_responses = client.send_offset_request(offset_requests)
        for r in offsets_responses:
            print "partition = %s, offset = %s"%(r.partition, r.offsets[0])


    def readMessage(self): 
        '''读取message　topic，暂时无用'''
        c = KafkaConsumer(self.messagetopic, auto_offset_reset='earliest',
                          bootstrap_servers=self.server)
        for m in c:
            v = json.loads(m.value)
            print m.key,v['message']['lineage']['table']
            #print v['message']['headers']['operation']

    def readDataTopic(self):
        #c = KafkaConsumer(self.datatopic, group_id='ar-group', bootstrap_servers=self.server, auto_offset_reset='earliest')
        c = KafkaConsumer(self.datatopic, group_id='ar-group',
                          bootstrap_servers=self.server,
                          consumer_timeout_ms=10000,
                          enable_auto_commit  = False,
                          auto_offset_reset = 'latest'
                         )
        #c = KafkaConsumer(bootstrap_servers=server)
        ##for p in c.partitions_for_topic(datatopic):
        #tp0 = TopicPartition(datatopic, 0) 
        #tp1 = TopicPartition(datatopic, 1) 
        #tp2 = TopicPartition(datatopic, 2) 
        #tp3 = TopicPartition(datatopic, 3) 
        #tp4 = TopicPartition(datatopic, 4) 
        #c.assign([tp3]
        #c.seek(tp3,10)
        #for msg in c:
        #    print msg
        #exit()

        rows=[]
        oldtb=''
        oldop=''
        tb =''
        op =''
        newbatch=True
        for m in c:
            if self.stopflag: break
            try:
                #尝试转换为json格式，并判断是否是ＡＲ数据
                v = json.loads(m.value)
            except:
                continue
            if v['magic'] == 'atMSG' and v['type']=='DT':
                #print self.changeSQL(m)
                tb,op,data = self.splitData(m)
                print tb,op,data
            if (tb==oldtb) and (op==oldop):
                newbatch = False
            else:
                newbatch = True
            if newbatch:
                #如果是新的batch
                if rows:
                    pass
                    #插入数据到目标
                    #print oldtb,oldop,rows
                rows=[]
                rows.append(data)
                oldtb = tb
                oldop = op
            else:
                rows.append(data)

        c.commit()
        c.close()

    def startsync(self):
        self.putAllMetaList()
        p = multiprocessing.Process(target = self.readDataTopic, args = ())
        self.ppool.append(p)
        p.daemon = True
        p.start()
        time.sleep(1)
        #p.terminate()

    def stop(self):
        self.stopflag = True

    def getchstatus(self):
        for p in self.ppool:
            print p.pid,p.name,p.is_alive()


    def test(self):
        start = time.time()
        print("Start: " + str(start))
        db = cx_Oracle.connect(self.targetdb['user'],self.targetdb['passwd'],self.targetdb['tns'])
        cur=db.cursor()
        cur.prepare('insert into k_artest(id,dt,sourcedt) values(:1,:2,:3)')
        rows=[]
        for i in range(10):
            rows.append((i,i,datetime.datetime.now()))
            #cur.execute('insert into k_artest(id) values(%d)' % (i))
            #cur.execute(None,{'id':i})
            #cur.execute('insert into k_artest(id) values(:id)',{'id':i})
        cur.executemany(None,rows)
        db.commit()
        stop = time.time()
        print("Stop: " + str(stop))
        print(str(stop-start) + "秒")

    def test2(self):
        start = time.time()
        print("Start: " + str(start))
        db = cx_Oracle.connect(self.targetdb['user'],self.targetdb['passwd'],self.targetdb['tns'])
        db2 = cx_Oracle.connect(self.targetdb['user'],self.targetdb['passwd'],self.targetdb['tns'])
        cur=db.cursor()
        cur2=db2.cursor()

        for i in range(25000):
            cur.execute('insert into k_artest(id) values(%d)' % (i))
            cur.execute('insert into k_artest2(id) values(%d)' % (i))
            cur2.execute('insert into k_artest(id) values(%d)' % (100000+i))
            cur2.execute('insert into k_artest2(id) values(%d)' % (100000+i))
        db.commit()
        db2.commit()
        stop = time.time()
        print("Stop: " + str(stop))
        print(str(stop-start) + "秒")

def saveoffset(fname,offset):
    with open(fname,'w') as outfile:
        json.dum(offset,outfile)


def readoffset(fname):
    f = open(fname)
    offset_config = f.load(f)
    return offset_config



#获取复制表的元数据
#putAllMetaList()
#print metalist
#readMessage()

#readDataTopic()

if __name__ == "__main__":
    r4k = Replicate4Kafka()
    r4k.putAllMetaList()
    r4k.readDataTopic()
    #r4k.putAllMetaList()
    #print r4k.metalist
    #r4k.readDataTopic()
    #r4k.startsync()
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

