package impatient.scala.chapter18.test

import scala.collection.mutable.ArrayBuffer

/**
  * Created by wpy on 17-6-25.
  */
class Document(private var title: String, private var author: String) {
  def setTitle(newTitle: String): this.type = {
    this.title = newTitle
    this
  }

  def setAuthor(newAuthor: String): this.type = {
    this.author = newAuthor
    this
  }

  override def toString: String = s"${this.getClass.getName}($title, $author)"
}

object Title

object Author

object Chapter

class Book(private var titleB: String, private var authorB: String,private val chapter: ArrayBuffer[String]) extends Document(titleB, authorB) {
  def addChapter(newChapter: String): this.type = {
    chapter += newChapter
    this
  }

  private var useNextArgsAs: Any = _

  def set(t: Title.type): this.type = {
    useNextArgsAs = t
    this
  }

  def set(t: Author.type): this.type = {
    useNextArgsAs = t
    this
  }

  def set(t: Chapter.type): this.type = {
    useNextArgsAs = t
    this
  }

  def to(t: String): this.type = {
    if (useNextArgsAs == Title) titleB = t
    else if (useNextArgsAs == Author) authorB = t
    else if (useNextArgsAs == Chapter) chapter += t
    else throw new IllegalArgumentException(s"wrong args, $Title/$Author/$Chapter expected but $useNextArgsAs detect")
    this
  }

  override def toString: String = s"${this.getClass.getName}($titleB, $authorB, [${chapter.mkString(",")}])"
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

    book set Title to "Scala for Impatient" set Author to "Cay Horstmann"
    println("===============================")
    println(book)
    println(doc)
  }
}