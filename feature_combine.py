import json
import csv
import sys

import os, time

from pyspark import SparkFiles, SparkContext, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, DoubleType, StringType, StructType


class Field(object):
    def __init__(self, name, tp):
        self.name = name
        self.tp = tp


class Relation(object):

    def __init__(self, a, b):
        """
        :type a:str
        :type b:str
        :return:
        """
        self.a = a
        self.b = b


class Config(object):

    def __init__(self, rel_table=None, csv_file=None, gen_table=None, rel_dict=None, gen_dict=None, delimiter=None):
        self.rel_table = rel_table
        self.csv_file = csv_file
        self.gen_table = gen_table
        self.rel_dict = rel_dict
        self.gen_dict = gen_dict
        self.delimiter = delimiter
        self.field_type = list() # type: list[Field]
        self.relation = list() # type: list[Relation]


class ConfigParser(object):

    @staticmethod
    def parse(path):
        with open(path, 'r') as f:
            config_json = json.load(f)
        cfg = Config()
        cfg.__dict__ = config_json
        cfg.relation = [Relation(rl['a'],rl['b']) for rl in cfg.relation]
        cfg.field_type = [Field(rl['name'], rl['tp']) for rl in cfg.field_type]
        return cfg


class FileUtil:

    @classmethod
    def path(cls, sc, filepath):
        path_class = sc._gateway.jvm.org.apache.hadoop.fs.Path
        path_obj = path_class(filepath)
        return path_obj

    @classmethod
    def get_file_system(cls,sc):
        filesystem_class = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
        hadoop_configuration = sc._jsc.hadoopConfiguration()
        return filesystem_class.get(hadoop_configuration)

    @classmethod
    def cp_and_append(cls,sc, s, ap, t):
        try:
            fs = cls.get_file_system(sc)
            s_file = cls.path(sc, s)
            t_file = cls.path(sc, t)
            if fs.exists(t_file):
                fs.delete(t_file, True)

            br_class = sc._gateway.jvm.java.io.BufferedReader
            ips_class = sc._gateway.jvm.java.io.InputStreamReader
            br = br_class(ips_class(fs.open(s_file)))

            bw_class = sc._gateway.jvm.java.io.BufferedWriter
            ops_class = sc._gateway.jvm.java.io.OutputStreamWriter
            bw = bw_class(ops_class(fs.create(t_file, True)))
            line = br.readLine()
            while line is not None:
                bw.write(line)
                bw.newLine()
                line = br.readLine()
            br.close()
            for add in ap:
                bw.write(add)
                bw.newLine()
            bw.flush()
            bw.close()
        except Exception as e:
            raise e


class FeatureCombine(object):

    def __init__(self, config_path):
        self.config = ConfigParser.parse(config_path)

    def run(self):
        spark = SparkSession \
            .builder \
            .enableHiveSupport() \
            .getOrCreate()

        # load table
        rel_table = self.load_hive(spark)
        rel_table.persist()
        print "table %s loaded" %self.config.rel_table
        csv_table = self.load_csv_p(spark)
        print "csv file %s loaded" %self.config.csv_file
        # join
        join_cond = list()
        partition_field = list()
        sc_joins = list()
        sc_joins_str = list()
        for rel in self.config.relation:
            join_cond.append(rel_table[rel.a] == csv_table[rel.b])
            partition_field.append(rel.a)
            sc_joins.append(csv_table[rel.b])
            sc_joins_str.append(rel.b)
        data = rel_table.join(csv_table, join_cond, how="left")
        data = data.drop(*tuple(sc_joins))
        print "table joined in memory"

        # save to hive
        data.write.mode("overwrite").partitionBy(*tuple(partition_field))\
            .saveAsTable(self.config.gen_table)
        print "saved to hive %s" %self.config.gen_table

        # dict join
        append = list()
        for field in self.config.field_type:
            name = field.name
            tp = field.tp
            if name not in sc_joins_str:
                append.append('"%s",%s' %(name, tp))
        FileUtil.cp_and_append(spark.sparkContext, self.config.rel_dict, append,self.config.gen_dict)
        print "generate dict %s" % self.config.gen_dict

    def load_hive(self, spark):
        data = spark.sql("select * from %s" % self.config.rel_table)
        return data

    def load_csv_p(self, spark):
        reader = csv.reader(open(self.config.csv_file,"r"), delimiter=",")
        un_order_header = dict()
        for field in self.config.field_type:
            if field.tp == 'numeric':
                un_order_header[field.name] = float
            else:
                un_order_header[field.name] = None

        header = []
        col_type = []
        for row in reader:
            for r in row:
                if r not in un_order_header:
                    raise Exception("column %s not found in configuration" %r)
                header.append(r)
                col_type.append(un_order_header[r])
            break
        col_num = len(header)

        i = 1
        data = list()
        for row in reader:
            if len(row) != col_num:
                raise Exception("data not consist with header:line %d, expect %d columns, found %d" %(i, col_num, len(row)))
            line = list()
            for r,nm,tp in zip(row, header, col_type):
                if tp is None:
                    line.append(r)
                else:
                    try:
                        r = r.strip()
                        if r == '':
                            line.append(None)
                        else:
                            line.append(tp(r))
                    except Exception as e:
                        line.append(None)
                        #raise Exception("line %d, column %s can not convert to float: %s" %(i, nm, r))
            data.append(tuple(line))
            i += 1
        assert isinstance(spark.sparkContext, SparkContext)
        rdd = spark.sparkContext.parallelize(data)
        data = SQLContext(spark.sparkContext).createDataFrame(rdd, header)
        print data
        return data



    def load_csv(self, spark):
        data_schema_list = list()
        for field in self.config.field_type:
            if field.tp == 'numeric':
                field_schema = StructField(field.name, DoubleType())
            else:
                field_schema = StructField(field.name, StringType())
            data_schema_list.append(field_schema)
        data_schema = StructType(data_schema_list)
        reader = spark.read
        spark.sparkContext.addFile(self.config.csv_file)
        # cur_dir = os.path.abspath(os.curdir)
        # csv_file = "local://%s" % os.path.join(cur_dir, self.config.csv_file)
        csv_file = "file://%s" % SparkFiles.get(self.config.csv_file)
        data = reader.csv(path=csv_file, sep=self.config.delimiter, schema=data_schema, header=True,
                          mode='FAILFAST')
        return data


cfg = sys.argv[1]
FeatureCombine(cfg).run()
