package impatient.scala.chapter18.test

import scala.collection.mutable.ArrayBuffer

/**
  * Created by wpy on 17-6-25.
  */
class Document(var title: String, var author: String) {
  def setTitle(newTitle: String): this.type = {
    title = newTitle
    this
  }

  def setAuthor(newAuthor: String): this.type = {
    author = newAuthor
    this
  }

  override def toString: String = s"${this.getClass.getName}($title, $author)"
}

class Book(title: String, author: String, chapter: ArrayBuffer[String]) extends Document(title, author) {
  def addChapter(newChapter: String): this.type = {
    chapter += newChapter
    this
  }

  override def toString: String = s"${this.getClass.getName}($title, $author, [${chapter.mkString(",")}])"
}

object Test {
  def main(args: Array[String]): Unit = {
    val book = new Book("book", "ss", ArrayBuffer.empty[String])
    val doc = new Document("doc", "aa")

    println(book)
    println(doc)

    book addChapter "c_1" setAuthor "aa" setTitle "newBook"
    doc setAuthor "ss" setTitle "new_doc"
    println("===============================")
    println(book)
    println(doc)
  }
}