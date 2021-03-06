package me.cjavellana.commons.pipeline

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.concurrent.TimeUnit
import kotlin.test.assertTrue

class InputPlusOneStage : AbstractStage() {
    override fun process(context: Context): Context {
        val input = context.get("input") as Int
        context.put("inputPlusOne", input + 1)
        return context
    }
}

open class InputTimesTenStage : AbstractStage() {
    override fun process(context: Context): Context {
        context.put("inputTimesTen", input(context) * 10)
        return context
    }

    protected open fun input(context: Context): Int = context.get("input") as Int
}

class LongRunningStage : AbstractStage() {

    override fun process(context: Context): Context {
        Thread.sleep(TimeUnit.SECONDS.toMillis(60))
        return context
    }
}

class UseInputPlusOneStageOutputThenMultiplyByTen : InputTimesTenStage() {
    override fun input(context: Context): Int = context.get("inputPlusOne") as Int
}

open class ThrowExceptionStage : AbstractStage() {
    override fun process(context: Context): Context {
        throw Exception("I just felt like throwing up..")
    }
}

internal class PipelineTest {

    @Test
    fun itMustExecuteSingleTaskSingleStagePipeline() {
        val pipeline = Pipeline()

        val req = Context()
        req.put("input", 1)

        pipeline.addStage(InputPlusOneStage())
        pipeline.process(req)

        assertEquals(2, req.get("inputPlusOne") as Int)
    }

    @Test
    fun itMustExecuteMultiTaskSingleStagePipeline() {
        val pipeline = Pipeline()

        val req = Context()
        req.put("input", 1)

        pipeline.addStage(InputPlusOneStage(), InputTimesTenStage())
        pipeline.process(req)

        assertEquals(2, req.get("inputPlusOne") as Int)
        assertEquals(10, req.get("inputTimesTen") as Int)
    }

    @Test
    fun itMustExecuteSingleTaskMultiStagePipeline() {
        val pipeline = Pipeline()

        val req = Context()
        req.put("input", 1)

        pipeline.addStage(InputPlusOneStage())
                .addStage(UseInputPlusOneStageOutputThenMultiplyByTen())
        pipeline.process(req)

        assertEquals(2, req.get("inputPlusOne") as Int)
        assertEquals(20, req.get("inputTimesTen") as Int)
    }

    @Test
    fun itCapturesUncaughtException() {
        val pipeline = Pipeline()

        val req = Context()
        req.put("input", 1)

        pipeline.addStage(InputPlusOneStage())
                .addStage(ThrowExceptionStage())
        pipeline.process(req)

        assertEquals(1, req.getExceptions().size)
    }

    @Test
    fun itTerminatesPipelineWhenTasksOverrunTimeout() {
        val pipeline = Pipeline()
                .withStageExecutionTimeoutMillis(100)
                .addStage(LongRunningStage())

        val startTime = System.currentTimeMillis()
        pipeline.process(Context())
        val elapsed = startTime - System.currentTimeMillis()

        // allow buffer time to avoid flaky test
        assertTrue { elapsed < 500 }
    }
}