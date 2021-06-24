package com.mob.dpi.beans

import scala.reflect.runtime.universe._

class CarrierFactory[T <: BaseCarrier] {


  def createCarrier(clazz : Class[T]) : Unit = {



  }
}
