# requirement

1. pip install kafka-python
2. pip install mysql-python
3. pip install cx_Oracle


# 基础说明
1. 从attunity Replicate 写入kafka的数据中读取变化量，并写入目标数据库
2. attunity replicate kafka中需要设置partition by message key,message key 需要使用schema+tablename配置
3. 程序根据topic中partition的数量使用多线程并行读取kafka,可以多设置一些partition,已增加并行读
4. 数据源的日志要完整，即befor image中要包含update的所有字段信息而不是只有更新的部分
5. 根据调用时的配置，决定读取指定表的变化量，而不是topic中所有的变化量




# 待完善
1. 目前只处理了ＰＫ这种关系，如果一个table没有pk,但有唯一索引，实际上也可以达到预期的效果，但目前没有处理
