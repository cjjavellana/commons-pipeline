package me.cjavellana.commons.pipeline

interface Stage {

    fun process(context: Context): Context

}