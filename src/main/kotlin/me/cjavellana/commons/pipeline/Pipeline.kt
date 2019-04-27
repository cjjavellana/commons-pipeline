package me.cjavellana.commons.pipeline

import java.util.*
import java.util.Collections.synchronizedList
import java.util.Collections.synchronizedMap
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.collections.set

@Suppress("UNCHECKED_CAST")
class Context {
    private val contextMap = synchronizedMap(mutableMapOf<Any, Any>(
            "exceptions" to synchronizedList(mutableListOf<Exception>())
    ))

    fun recordException(e: Exception) {
        (contextMap["exceptions"] as MutableList<Exception>).add(e)
    }

    fun get(key: Any) = contextMap[key]
    fun getOrDefault(key: Any, default: Any) = contextMap.getOrDefault(key, default)
    fun put(key: Any, value: Any): Context {
        // TODO Log warning when overwriting a key
        contextMap[key] = value
        return this
    }

    fun getExceptions(): List<Exception> = contextMap["exceptions"] as MutableList<Exception>
}

/**
 * Executes tasks or stages sequentially. The order of the execution flow is defined
 * by the order in which the stages are added into the pipeline
 *
 */
class Pipeline {

    private val stages = LinkedList<List<Stage>>()
    private var stageExecutionTimeout = 300L

    fun withStageExecutionTimeout(timeoutSecs: Long): Pipeline {
        stageExecutionTimeout = timeoutSecs
        return this
    }

    /**
     * Adds an execution stage into the pipeline.
     *
     * Stages that are added together will be executed in parallel.
     */
    fun addStage(vararg stages: Stage): Pipeline {
        this.stages.add(stages.toList())
        return this
    }

    fun process(context: Context): Context {
        for (parallelSteps in stages) {
            executeTasksInParallel(context, parallelSteps)
        }

        return context
    }

    private fun executeTasksInParallel(context: Context, tasks: List<Stage>) {
        val executorService = executorService(tasks.size)
        for (step in tasks) {
            executorService.submit {
                try {
                    step.process(context)
                } catch (e: Exception) {
                    context.recordException(e)
                }
            }
        }

        executorService.shutdown()
        executorService.awaitTermination(stageExecutionTimeout, TimeUnit.SECONDS)
    }

    private fun executorService(workers: Int) = Executors.newFixedThreadPool(workers)
}