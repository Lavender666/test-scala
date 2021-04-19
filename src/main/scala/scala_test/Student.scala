package scala_test

object Student{
}

class Student{
  //get  set
  var id=666;
  //get
  val age:Int=18
  //只有伴生对象才能访问得到
  private var name:String="Lee"
  //只有类的内部自己能访问，连伴生对象都搞不定
  private[this] var address:String="Dali"

}
