package me.cjavellana.commons.pipeline

abstract class AbstractStage {

    open fun getName(): String = "${this.javaClass.name}-${Thread.currentThread().id}"

    abstract fun process(context: Context): Context

}