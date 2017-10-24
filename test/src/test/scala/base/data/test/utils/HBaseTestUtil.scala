package base.data.test.utils

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{HBaseConfiguration, NamespaceDescriptor, NamespaceNotFoundException, TableName}
import org.apache.hadoop.hbase.client.{ColumnFamilyDescriptorBuilder, ConnectionFactory, TableDescriptorBuilder}
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles
import org.apache.hadoop.hbase.util.Bytes

class HBaseTestUtil {

  val conf = HBaseConfiguration.create()
  val conn = ConnectionFactory.createConnection(conf)
  val admin = conn.getAdmin

  implicit def toBytes(src: String) = Bytes.toBytes(src)

  def createTable(name: String, cf: Seq[String])(implicit bytes: String => Array[Byte]) = {
    val table = TableDescriptorBuilder.newBuilder(TableName.valueOf(name))
    val cfs = cf.map(col => ColumnFamilyDescriptorBuilder.newBuilder(bytes(col)).build()).foreach(table.addColumnFamily)
    val nameSpace = try
      admin.getNamespaceDescriptor("wpy")
    catch {
      case e: NamespaceNotFoundException =>
        NamespaceDescriptor.create("wpy").build()
    }
    admin.createNamespace(nameSpace)
    admin.createTable(table.build())
  }

  def loadFromHFile(name: String, path: String) = {
    val table = conn.getTable(TableName.valueOf(name))
    val load = new LoadIncrementalHFiles(conf)
    load.doBulkLoad(new Path(path), admin, table, conn.getRegionLocator(TableName.valueOf(name)), false, false)
  }

  def truncateTable(name: String,pre:Boolean) = {
    admin.disableTable(TableName.valueOf(name))
    admin.truncateTable(TableName.valueOf(name), pre)
  }

  def close = {
    admin.close()
    conn.close()
  }

}
