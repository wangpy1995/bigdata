package mapreduce.utils

import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos
import org.apache.hadoop.hbase.util.Base64
import org.apache.spark.internal.Logging

/**
  * Created by wangpengyu6 on 2017/7/17.
  */
object HBaseUtils extends Logging {

  def convertStringToScan(str: String): Scan = {
    val scan = Base64.decode(str)
    ProtobufUtil.toScan(ClientProtos.Scan.parseFrom(scan))
  }

  def convertScanToString(scan: Scan): String = {
    Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray)
  }

}
