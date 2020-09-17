package com.atguigu.other.streaming

import java.io.DataOutputStream

import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.compress.{CompressionCodec, GzipCodec}
import org.apache.hadoop.mapred.lib.MultipleOutputFormat
import org.apache.hadoop.mapred.{FileOutputFormat, JobConf, RecordWriter, TextOutputFormat}
import org.apache.hadoop.util.{Progressable, ReflectionUtils}


class AppendTextOutputFormat extends TextOutputFormat[Any,Any]{
  override def getRecordWriter(ignored: FileSystem, job: JobConf, iname: String, progress: Progressable): RecordWriter[Any, Any] = {
    //查看是否压缩
    val isCompressed: Boolean = FileOutputFormat.getCompressOutput(job)
    //指定默认分隔符
    val keyValueSeparator = job.get("mapreduce.output.textoutputformat.separator","\t")
    //自定义输出文件名
    val name = job.get("filename",iname)
    //判断是否压缩
    if (! isCompressed) {
      //file是临时路径
      val file: Path = FileOutputFormat.getTaskOutputPath(job,name)
      val fs: FileSystem = file.getFileSystem(job)
      //最终要输出的路径
      val newFile = new Path(FileOutputFormat.getOutputPath(job),name)
      val fileOut : FSDataOutputStream = if (fs.exists(newFile)) {
        //存在，追加写
        fs.append(newFile)
      } else {
          //不存在，创建
        fs.create(file, progress)
      }
      //返回
      new TextOutputFormat.LineRecordWriter[Any, Any](fileOut, keyValueSeparator)
    }else {
      //如果没有定义压缩格式默认采用Gizp
      val codecClass: Class[_ <: CompressionCodec] = FileOutputFormat.getOutputCompressorClass(job, classOf[GzipCodec])
      // create the named codec(实例化)
      val codec: CompressionCodec = ReflectionUtils.newInstance(codecClass, job)
      // codec.getDefaultExtension 加上压缩扩展名
      val file: Path = FileOutputFormat.getTaskOutputPath(job, name + codec.getDefaultExtension)
      val fs: FileSystem = file.getFileSystem(job)
      val newFile: Path = new Path(FileOutputFormat.getOutputPath(job), name + codec.getDefaultExtension)

      val fileOut: FSDataOutputStream = if (fs.exists(newFile)) {
        //存在，追加写
        fs.append(newFile)
      } else {
        //不存在，创建
        fs.create(file, progress)
      }
      new TextOutputFormat.LineRecordWriter[Any, Any](new DataOutputStream(codec.createOutputStream(fileOut)), keyValueSeparator)
    }
  }
}
class RDDMultipleAppendTextOutputFormat extends MultipleOutputFormat[Any, Any]{
  private var theTextOutputFormat: AppendTextOutputFormat = null

  //产生分区目录
  //override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String ={
  //
  //  //TODO 分区目录
  //}

  //追加写
  override def
  getBaseRecordWriter(fs: FileSystem, job: JobConf, name: String, arg3: Progressable): RecordWriter[Any, Any] = {
    if (this.theTextOutputFormat == null) {
      this.theTextOutputFormat = new AppendTextOutputFormat()
    }
    this.theTextOutputFormat.getRecordWriter(fs, job, name, arg3)
  }
  //key重置为空
  override def generateActualKey(key: Any, value: Any): Any =
    NullWritable.get()
}
