# Commons-Pipeline
Simple library for implementing sequential task execution

## Usage
```kotlin
val context = Context()
val sharedQueue = BlockingQueue()
val pipeline = new Pipeline()
    .add(ValidateStage())
    .add(ReadDataFile(sharedQueue), SaveDataStage(sharedQueue)) // this stage will be executed in parallel
    .add(DispatchNotificationStage())
pipeline.process(context)
```         