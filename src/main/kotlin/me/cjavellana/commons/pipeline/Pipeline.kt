package me.cjavellana.commons.pipeline

import org.slf4j.LoggerFactory
import java.util.*
import java.util.Collections.synchronizedList
import java.util.Collections.synchronizedMap
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.collections.set

@Suppress("UNCHECKED_CAST")
class Context {
    private val logger = LoggerFactory.getLogger(Context::class.java)

    private val contextMap = synchronizedMap(mutableMapOf<Any, Any>(
            "exceptions" to synchronizedList(mutableListOf<Exception>())
    ))

    fun recordException(e: Exception) {
        (contextMap["exceptions"] as MutableList<Exception>).add(e)
    }

    fun get(key: Any) = contextMap[key]
    fun getOrDefault(key: Any, default: Any) = contextMap.getOrDefault(key, default)
    fun put(key: Any, value: Any): Context {
        if (contextMap[key] != null) {
            logger.warn("Overwriting key {}. Old Value {} New Value", key, contextMap[key], value)
        }

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
    companion object {
        private val logger = LoggerFactory.getLogger(Pipeline::class.java)
    }

    private val stages = LinkedList<List<AbstractStage>>()
    private var stageExecutionTimeout = TimeUnit.MINUTES.toMillis(5)
    private val currentStage = AtomicInteger(0)

    fun withStageExecutionTimeoutMillis(timeout: Long): Pipeline {
        stageExecutionTimeout = timeout
        return this
    }

    /**
     * Adds an execution stage into the pipeline.
     *
     * Stages that are added together will be executed in parallel.
     */
    fun addStage(vararg stages: AbstractStage): Pipeline {
        this.stages.add(stages.toList())
        return this
    }

    fun process(context: Context): Context {
        for (parallelSteps in stages) {
            currentStage.incrementAndGet()
            executeTasksInParallel(context, parallelSteps)
        }

        return context
    }

    private fun executeTasksInParallel(context: Context, tasks: List<AbstractStage>) {
        val executorService = executorService(tasks.size)
        for (step in tasks) {
            executorService.submit {
                try {
                    step.process(context)
                } catch (e: Exception) {
                    logger.error("Stage ${step.getName()} threw an exception", e)
                    context.recordException(e)
                }
            }
        }

        executorService.shutdown()
        executorService.awaitTermination(stageExecutionTimeout, TimeUnit.MILLISECONDS)
    }

    private fun executorService(workers: Int) = Executors.newFixedThreadPool(workers)
}