# Claude Documentation Guidelines for Scoop

This document provides comprehensive guidelines for documenting the Scoop codebase, which contains a proof-of-concept implementation of **structured cooperation** for distributed, message-based systems.

## Table of Contents

- [About This Project](#about-this-project)
- [Understanding Structured Cooperation](#understanding-structured-cooperation)
- [Documentation Philosophy](#documentation-philosophy)
- [Key Concepts & Terminology](#key-concepts--terminology)
- [Important Documentation TODOs](#important-documentation-todos)
- [Component Documentation Guidelines](#component-documentation-guidelines)
- [Code Architecture Overview](#code-architecture-overview)
- [Essential Reading](#essential-reading)

## About This Project

Scoop is a proof-of-concept implementation of **structured cooperation** - an approach to solving common problems in distributed systems by applying principles similar to structured concurrency. The codebase serves as an **illustration and learning resource** for programmers of all backgrounds, which means documentation must be exceptionally clear, patient, and thorough.

### Target Audience
- Programmers with varying levels of distributed systems experience
- Students learning about concurrency and distributed systems
- Engineers interested in alternatives to eventual consistency
- Researchers exploring structured approaches to distributed computing

### Key Resources
Before documenting anything, familiarize yourself with the [project README](README.md), especially the part that discusses in what order the code should be read and understood. Assume this ordering when writing documentation.

## Understanding Structured Cooperation

### The Core Principle

The fundamental rule of structured cooperation is:

> **"When they reach the end of a step in which messages were emitted, sagas suspend their execution and don't continue with the next step until all handlers of those messages have finished executing."**

This simple rule eliminates many traditional distributed systems problems:
- **Eventual consistency issues** - All handlers of emitted messages are guaranteed to finish before proceeding
- **Difficult error tracing** - Exceptions propagate across service boundaries with distributed stack traces
- **Complex message coordination** - Dependencies are explicit and automatically enforced
- **Unpredictable system state** - The entire system state is consistent at step boundaries

### Orchestration vs Choreography

Structured cooperation occupies a unique position between traditional approaches:

- **Orchestrated** aspects: Sagas are explicit - you can see the entire saga in one place in the codebase
- **Choreographed** aspects: Services don't directly call each other - they emit messages without processing responses
- **Cooperative** synchronization: The key difference is the suspension rule that ensures proper ordering

### Key Benefits
- **Distributed exceptions and stack traces** - Errors propagate across service boundaries
- **Predictable execution order** - Dependencies are explicit and enforced
- **Automatic rollbacks** - Compensating actions execute in reverse order
- **Resource management** - Try-finally semantics work across services
- **Eliminated race conditions** - Synchronization is built into the protocol

### Implementation Overview
Scoop implements structured cooperation using:
- **Cooperation Lineage**: UUID-based message hierarchy tracking
- **Message Events**: Database-driven saga execution state management
- **EventLoopStrategy**: Customizable handler topology and execution conditions
- **Rollback Mechanism**: Compensating actions with nested rollback support
- **CooperationContext**: Shared data propagation across message hierarchies

### Technical Foundation
- **Language**: Kotlin (minimal fancy features for accessibility)
- **Framework**: Quarkus (for dependency injection)
- **Database**: Postgres for everything (message queue + state tracking)
- **Approach**: Raw SQL, minimal abstraction, ~500 lines of core SQL logic
- **Philosophy**: Proof-of-concept focused on conveying ideas clearly

### Basic Saga Structure

When documenting saga patterns, reference this fundamental structure:

```kotlin
val myHandler = saga(name = "my-handler") {
    step { scope, message ->
        // Step 1: Do work, emit messages
        scope.launch("some-topic", JsonObject().put("data", "value"))
    }
    
    step { scope, message ->
        // Step 2: Only runs after ALL handlers of messages 
        // from step 1 have completed
    }
}

// Subscribe the handler to a topic
messageQueue.subscribe("input-topic", myHandler)
```

### Key Patterns to Document

**Rollback with Compensating Actions**:
```kotlin
step(
    invoke = { scope, message -> 
        // Forward logic
        doSomething()
        scope.launch("next-topic", message)
    },
    rollback = { scope, message, throwable ->
        // Compensating action - runs in reverse order if failure occurs
        undoSomething()
    }
)
```

**Resource Management**:
```kotlin
tryFinallyStep(
    invoke = { scope, message ->
        resourceService.acquire("resource-123")
        scope.launch("do-work", message)
    },
    finally = { scope, message ->
        resourceService.release("resource-123")
    }
)
```

**Independent Operations** (when you don't want structured cooperation):
```kotlin
step { scope, message ->
    // This launches independently - no waiting, no rollback coordination
    scope.launchOnGlobalScope("independent-topic", message)
}
```

**CooperationContext Usage**:
```kotlin
data object MyContextKey : CooperationContext.MappedKey<MyContextValue>()
data class MyContextValue(val value: Int) : CooperationContext.MappedElement(MyContextKey)

saga("root-handler") {
    step { scope, message -> 
        scope.context += MyContextValue(3)
        scope.launch("child-topic", JsonObject())
    }
    
    step { scope, message ->
        // Context persists across steps - logs 3
        log.info(scope.context[MyContextKey]!!.value) 
    }
}

saga("child-handler") {
    step { scope, message -> 
        // Context propagated from parent - logs 3
        log.info(scope.context[MyContextKey]!!.value) 
        
        scope.context += MyContextValue(10) // Only affects this saga and its children
    }
}
```

### Historical Context & Theoretical Foundation

**Understanding the Evolution: From GOTO to Structured Cooperation**

Structured cooperation represents the latest iteration of a fundamental principle that has shaped programming language design for over half a century. To understand why it's so effective, it's essential to trace this historical progression and understand the common thread that connects each evolution.

#### The GOTO Problem & The Black Box Rule

In the 1960s, programming languages commonly included the `GOTO` statement, which allowed execution flow to jump from any point in a program to any other point. While this provided ultimate flexibility, it created a fundamental problem: programs could not be organized into a proper hierarchy.

**The Core Issue**: When any piece of code can jump to any other piece of code anywhere in the program, you lose the ability to reason about code in terms of "black boxes." To understand what a program does, you must examine every single instruction across the entire execution path, because any one of them could redirect the flow elsewhere.

**The Black Box Rule**: For code to be maintainable and understandable, whenever execution flow goes *into* something (a function, if statement, loop, etc.), it must be guaranteed to always, at some point, *come back out*. This creates a hierarchy where you can reason about components without needing to understand their internal implementation.

E. W. Dijkstra formalized this in his 1968 paper "Go To Statement Considered Harmful," arguing that `GOTO` should be eliminated from programming languages. The industry initially resisted, but eventually recognized that restricting what programmers could do actually enabled more powerful features:

- **Call stacks and stack traces** - Made possible by execution flow hierarchy
- **Exception handling with stack unwinding** - Depends on predictable flow structure  
- **Resource management constructs** (try-with-resources) - Requires guaranteed entry/exit points
- **Compiler optimizations** - Enabled by flow structure guarantees

#### Structured Concurrency: The Same Problem Returns

Fast forward to the era of concurrent programming. Languages introduced primitives like `go` (Go), `new Thread()` (Java), etc., that spawn separate threads of execution. These mechanisms suffered from eerily similar problems to `GOTO`:

- **Difficult to reason about** - No guarantees about thread lifecycle or relationships
- **Poor error handling** - Exceptions in spawned threads often just disappeared
- **Resource management issues** - Spawned threads could outlive their parent's resources
- **Debugging nightmares** - Stack traces only showed local thread state, not spawn context

**The Control Flow Problem**: Thread spawning mechanisms violate the black box rule by splitting execution into multiple strands, with only one strand returning to the caller. This creates the same hierarchy-breaking problem as `GOTO`, but worse—instead of one chaotic execution path, you now have multiple paths squiggling unpredictably through the codebase.

**Structured Concurrency Solution**: The solution mirrors structured programming's approach—restrict thread spawning to occur only within special code blocks (like `coroutineScope` in Kotlin, `Nursery` in Trio, `StructuredTaskScope` in Java). The crucial rule: **the code block does not exit until everything it spawned has finished running.**

This restoration of the black box rule enables the same benefits as eliminating `GOTO`:
- **Predictable thread hierarchy** - Clear parent-child relationships
- **Proper error propagation** - Exceptions bubble up the hierarchy with distributed stack traces
- **Resource management** - Resources remain available until all children complete
- **Cancellation/timeout support** - Well-defined hierarchy for propagating cancellation signals

#### Structured Cooperation: Distributed "GOTO" Considered Harmful

Distributed message-based systems suffer from the same fundamental problem, but amplified:

- **GOTO**: One execution strand squiggling across one codebase
- **Unstructured concurrency**: Multiple strands squiggling across one codebase  
- **Unstructured distributed systems**: Unknown numbers of strands squiggling across unknown numbers of codebases

**The "Spaghettopus Code" Problem**: When a service emits a message, you have no idea if, how many, or which listeners it might trigger. Each listener represents a new branch of execution running in a completely different codebase. The result is a "spaghettopus" of execution flow—multiple tangled strands spread across multiple services with no hierarchy or coordination.

**Structured Cooperation Solution**: Apply the same principle as structured concurrency, but to distributed systems. The fundamental rule: **message handlers suspend execution and don't continue until all handlers of emitted messages have finished executing.**

This creates a hierarchy of message handlers that obeys the black box rule, enabling the same benefits we've seen before:

- **Distributed stack traces** - Exceptions propagate across service boundaries with full context
- **Distributed stack unwinding** - Rollbacks execute in reverse order across services
- **Resource management** - Try-finally semantics work across service boundaries
- **Predictable execution order** - Dependencies are explicit and automatically enforced
- **Eliminated eventual consistency** - All handlers complete before proceeding

#### Why This Pattern Is "Unreasonably Effective"

The recurring pattern across all three domains reveals a fundamental principle: **restricting the set of permissible programs allows you to rely on patterns that become enforced by those restrictions, enabling powerful new features.**

Each evolution follows the same pattern:
1. **Identify chaos-inducing primitive** (GOTO, thread spawning, message emission)
2. **Recognize hierarchy violation** (breaks black box rule)
3. **Impose structural constraints** (eliminate GOTO, structured concurrency blocks, cooperation suspension)
4. **Recover lost capabilities** (stack traces, error handling, resource management)
5. **Enable new features** (compiler optimizations, cancellation, distributed transactions)

**Standing on Giants' Shoulders**: Structured cooperation builds directly on the work of:
- **Nathaniel J. Smith** - Articulated structured concurrency principles and the black box rule
- **Roman Elizarov & Kotlin team** - Implemented practical structured concurrency with coroutines
- **E. W. Dijkstra** - Established the original structured programming foundations

#### Implications for Documentation

When documenting Scoop components, emphasize:

- **Hierarchy preservation** - How each component maintains the execution flow hierarchy
- **Black box behavior** - What guarantees each component provides to its callers
- **Cooperation vs independence** - When to use structured cooperation vs global scope
- **Historical parallels** - How distributed problems mirror earlier programming language challenges
- **Constraint benefits** - How restrictions enable rather than limit capabilities

This historical context helps programmers understand that structured cooperation isn't a novel invention, but rather the natural application of proven principles to a new domain. The effectiveness comes not from cleverness, but from applying a time-tested pattern that has consistently improved programming across multiple paradigms.

## Documentation Philosophy

When documenting Scoop code, adopt a **"kind patient teacher"** mindset:

### Core Principles
1. **Assume minimal prior knowledge** - Explain concepts from first principles
2. **Be specific and clear** - Avoid ambiguous language and undefined terms
3. **Provide context** - Explain *why* something works this way, not just *how*
4. **Reference extensively** - Link to blog posts and README rather than repeating content
5. **Use concrete examples** - Abstract concepts need practical illustrations
6. **Progressive disclosure** - Start simple, then add complexity

### Documentation Tone
- **Patient and encouraging** - Never assume readers "should know" something
- **Precise but accessible** - Technical accuracy without unnecessary jargon
- **Humble about complexity** - Acknowledge when things are difficult or subtle
- **Reference-rich** - Guide readers to deeper understanding

### What to Document
- **Purpose and motivation** for each component
- **Relationship to structured cooperation principles** (suspension rule, cooperation lineage, etc.)
- **Integration points** with other components
- **Common usage patterns** with concrete code examples (see patterns above)
- **Edge cases** and error scenarios (rollback chains, multiple handler failures)
- **Exception propagation behavior** with JSON structure examples
- **Performance considerations** where relevant (message event table growth, etc.)
- **Cooperation vs independence trade-offs** (when to use global scope)

### What NOT to Document
- **Implementation details** that change frequently
- **Duplicate information** already well-covered in blog posts or README
- **Language-specific trivia** unless directly relevant to structured cooperation
- **Obvious functionality** that provides no learning value

## Key Concepts & Terminology

When documenting, ensure these terms are clearly defined for readers:

### Fundamental Concepts

**Structured Cooperation**
- A protocol for distributed systems based on synchronization rules
- Inspired by structured programming and structured concurrency
- Eliminates eventual consistency through explicit dependency management

**Saga**
- A sequence of transactional steps representing a distributed business operation
- Each step can emit messages to other services
- Supports compensating actions for rollback scenarios

**Cooperation Lineage**
- UUID-based hierarchy tracking message parent-child relationships
- Enables querying related message events across service boundaries
- Foundation for distributed exception propagation

**Message Events**
- Database records tracking saga execution state changes
- Types: EMITTED, SEEN, SUSPENDED, COMMITTED, ROLLING_BACK, etc.
- Used to determine when sagas can proceed to next steps

**Continuation**
- The executable portion of a saga from current state to completion
- Built from coroutine state and step definitions
- Represents a "slice" of execution that can be resumed

**Event Loop**
- Component managing saga execution lifecycle
- Performs "ticks" - fetching state, building continuations, executing steps
- Handles both forward progress and rollback scenarios

### Concepts

**CooperationContext**
- Shared data mechanism across message hierarchies using key-value pairs
- Similar to Kotlin's CoroutineContext - associates keys (CooperationContext.MappedKey) to values (CooperationContext.MappedElement)
- **Propagation rules**:
  - Parent to child: Context passes down when launching child messages
  - Child to parent: Changes in children do NOT propagate back to parents
  - Mutation: Sagas can modify their own context, visible in subsequent steps
  - Combining: `scope.launch()` accepts context parameter combined with existing context
- **Rollback behavior**: Context traverses back through steps in reverse during rollbacks
- **Key features enabled**: deadlines, sleep(), scheduling, resource management
- **Performance**: Lazy deserialization - minimal cost unless actually accessed

**EventLoopStrategy**
- Interface defining when sagas should resume or be abandoned
- Addresses the fundamental question: "Which handlers should we wait for?"
- Provides five key methods for customizable execution policies:
  - `resumeHappyPath`: Determines when saga is ready to continue normally
  - `giveUpOnHappyPath`: Decides when to abandon a saga during normal execution
  - `resumeRollbackPath`: Determines when saga is ready to continue rollback
  - `giveUpOnRollbackPath`: Decides when to abandon a saga during rollback
  - `start`: Customizes conditions for reacting to messages (e.g., ignore old messages)
- Returns raw SQL injected into queries that check saga readiness
- Enables features like sleep(), deadlines, cancellation as natural extensions
- Handles handler topology discovery and maintenance challenges

**Distributed Coroutine**
- Scoop's representation of a saga implementation
- Contains ordered list of TransactionalSteps
- Supports both happy path and rollback execution

**Distributed Exceptions & Stack Traces**
- Exceptions propagate from child sagas to parent sagas across service boundaries
- Each service receives a JSON exception with full distributed stack trace
- Parent handlers wait for children, enabling exception bubbling
- Stack traces show the cooperation lineage path the error traveled

**Global Scope & Independence**
- `launchOnGlobalScope()` creates independent message hierarchies
- Used when you don't want structured cooperation synchronization
- Enables gradual adoption - only part of system needs to use structured cooperation
- Independent hierarchies can still use structured cooperation internally

**Handler Topology & Discoverability**
- **Discoverable systems**: Can determine which components participate, what topics they listen to, and what messages they emit
- **CAP theorem implications**: Non-discoverable systems cannot guarantee consistency, only availability
- **Topology building requirements**:
  - Services must publish handler information (automated from code preferred)
  - Central registry needed (database, service registry, or custom solution)
  - Registry becomes SPOF requiring high availability
- **Deployment challenges**:
  - Cache invalidation when topology changes
  - Coordination of affected services during deployments
  - Pausing/unpausing message processing during topology updates
  - Waiting for in-flight messages to complete before deploying changes

**Message Events Table Structure**
- **Core columns**: message_id, type, coroutine_name, coroutine_identifier, step, cooperation_lineage
- **Additional columns**: created_at (timestamp), exception (for rollback scenarios), context (shared data)
- **Query patterns**: Structured cooperation enforcement through SQL queries checking event completion
- **Event sequence examples**: EMITTED → SEEN → SUSPENDED → COMMITTED for normal flow
- **Rollback sequences**: ROLLING_BACK → SUSPENDED → ROLLBACK_EMITTED → ROLLED_BACK

**Deadlines & Scheduling**
- **Deadlines implementation**: Three CooperationContext keys for different deadline types
  - Happy path deadline: Maximum time for normal execution
  - Rollback path deadline: Maximum time for rollback completion  
  - Combined deadline: Overall time limit for entire operation
- **Deadline propagation**: Inherited by child messages, parents can set stricter deadlines for children
- **Deadline tracing**: Mechanism to determine where a deadline originated
- **Sleep implementation**: Uses dedicated handler listening to sleep topic with custom EventLoopStrategy
- **Scheduling enabled**: "Send survey 2 days after purchase" = saga that sleeps then acts
- **Periodic execution**: Saga sleeps, then uses `launchOnGlobalScope` to restart itself

For detailed explanations of these concepts, refer to the [project glossary](README.md#glossary-of-terms) and [blog posts](#essential-reading).

## Important Documentation TODOs

The following TODOs exist in the codebase and should be addressed when documenting:

### High Priority Documentation Tasks

**CooperationScope & Context**
- Document that scope stays the same for entire coroutine run (including ROLLING_BACK)
- Explain that contexts are lazily deserialized for performance
- Document custom plus logic behavior for unused context keys

**Continuation & Step Management** 
- Document suspension points (after last step finished, before new step starts)
- Explain that continuation is equivalent to the scope
- Document that emitted events are written at end of each step regardless of outcome

**Rollback & Error Handling**
- Document that rollback failures take precedence over original exceptions (unlike Java suppressed exceptions)
- Explain partial rollback support and when last_parent_event check is skipped
- Document that ROLLING_BACK with null step indicates copied from parent

**Event Loop Strategy**
- Document why specific event types ('SEEN', 'SUSPENDED', 'COMMITTED', 'ROLLING_BACK') are used
- Explain that giving up is done by scheduling task (checked before resumption)
- Document handler topology discovery problem and HandlerRegistry solution

**Message Queue & Threading**
- Document lock gymnastics necessary for concurrent handler execution
- Explain configuration options for message processing
- Document automatic subscription for Sleep functionality

### TODO Items to Address

When documenting components, address these specific TODOs found in the code:

```
// TODO: Doc that the SIMPLE name is used (CooperationContext.kt:97)
// TODO: Doc that the scope stays the same for entire coroutine run! (CooperationScope.kt:50)
// TODO: DOC that Definition of this should actually be part of step (CooperationScope.kt:162)
// TODO: Doc that the suspension points are just after the last step finished (Continuation.kt:7)
// TODO: Doc that Emitted at the end of each step no matter what (Continuation.kt:29)
// TODO: Doc that, unlike in Java suppressed exceptions, rollback failures take precedence (RollbackState.kt:34)
// TODO: Doc that only deserialize when needed, so small cost unless you actually use it (CooperationContextMap.kt:11)
// TODO: Doc that giving up is done by scheduling the task (EventLoopStrategy.kt:31)
// TODO: Doc why 'SEEN', 'SUSPENDED', 'COMMITTED', 'ROLLING_BACK' (EventLoopStrategy.kt:136)
// TODO: Doc what these states mean (DistributedCoroutine.kt:8)
// TODO: Doc that whatever happens here is, conceptually, part of the same step as invoke (DistributedCoroutine.kt:92)
// TODO: doc that the continuation is equivalent to the scope (CooperationContinuation.kt:17)
// TODO: Doc that this is so that emitted & rollback correctly mark steps (CooperationContinuation.kt:42)
// TODO: Doc that the lock gymnastics are necessary (MessageEventRepository.kt:292)
// TODO: Doc that we don't do the last_parent_event check for rollbacks (MessageEventRepository.kt:302)
// TODO: Doc why these (PendingCoroutineRunSql.kt:286)
// TODO: DOC THE step is null which is true only when it's copied from the parent (PendingCoroutineRunSql.kt:304)
```

**Important Note**: When addressing TODOs, if you're unsure about the exact meaning or implications, add `"TODO CLAUDE: NOT SURE"` below the original TODO and don't attempt to document it. Keep all original TODOs in place for human review.

### Rollback Behavior Patterns

When documenting rollback functionality, reference these key behaviors:

**Distributed Stack Unwinding**:
- Rollbacks execute in reverse order (like stack unwinding)
- Child sagas roll back before parent sagas
- Multiple child handlers: failing child triggers rollback of successful siblings
- Nested rollbacks: if step emitted messages, their handlers roll back first

**Exception Precedence**:
- Rollback failures take precedence over original exceptions (unlike Java suppressed exceptions)
- Multiple failures: rollback exceptions are preferred in error reporting
- Partial rollbacks: some steps may have no compensating action needed

**Transaction Boundaries**:
- If exception thrown before transaction commits, standard DB rollback occurs
- Compensating actions only run for successfully committed steps
- Each step runs in its own transaction boundary

**Multiple Handler Scenarios**:
- If one handler fails, all successful sibling handlers are rolled back
- Parent waits for all children (successful and failed) before proceeding
- Rollback coordination maintains structured cooperation properties

**CooperationFailure & CooperationException**:
- **CooperationFailure**: Language-agnostic data structure for representing failures across services
- Contains type, message, stack trace, and list of causes - serialized in `exception` column
- **CooperationException**: JVM-specific translation of CooperationFailure for Scoop
- Enables distributed exception propagation between services in different languages
- Avoid using exception types for business logic - represents procedural coupling, not causal coupling

**Advanced Rollback Features**:
- **handleChildErrors parameter**: Third lambda parameter in `step()` for handling child exceptions
- Acts like a `catch` block around all child executions from that step
- Can emit messages for retries or ignore failures by returning normally
- Default implementation re-throws all child exceptions
- **Cancellation requests**: Implemented via `CANCELLATION_REQUESTED` event type
- **Undo/rollback requests**: Can roll back completed sagas by writing `ROLLBACK_EMITTED`
- **Rollback event sequence**: `ROLLING_BACK` → `SUSPENDED` → `ROLLBACK_EMITTED` → `ROLLED_BACK`
- **Rollback failure handling**: `ROLLBACK_FAILED` written if compensating action throws exception

### Distributed Exception Structure

When documenting exception handling, reference the JSON structure that propagates across services:

```json
{
  "type": "com.example.ChildRolledBackException",
  "causes": [
    {
      "type": "com.example.MyBusinessException", 
      "causes": [],
      "source": "grandchild-handler[uuid-here]",
      "message": "Business rule violation",
      "stackTrace": [
        {
          "fileName": "BusinessLogic.kt",
          "className": "com.example.BusinessLogic", 
          "lineNumber": 42,
          "functionName": "validateBusinessRule"
        }
      ]
    }
  ],
  "source": "parent-handler[uuid-here]",
  "message": "Child failure occurred while suspended in step [Step Name]",
  "stackTrace": []
}
```

**Key Fields**:
- `source`: Handler name + unique instance UUID for horizontal scaling
- `causes`: Nested exception chain showing distributed propagation path
- `message`: Includes step name where suspension/failure occurred
- `stackTrace`: Local stack trace where exception was caught/rethrown

## Component Documentation Guidelines

### Blocking Implementation Focus

Currently, focus documentation efforts on the **blocking** and **shared** components only. The reactive implementation will be documented separately.

### Primary Components to Document

**Core Execution Engine**
- `EventLoop.kt` - Main execution coordinator
- `DistributedCoroutine.kt` - Saga representation and lifecycle
- `CooperationScope.kt` - Execution context and capabilities

**Message & State Management**
- `PostgresMessageQueue.kt` - Message emission and subscription
- `MessageEventRepository.kt` - State persistence and querying
- `PendingCoroutineRunSql.kt` - Structured cooperation query logic

**Continuation System**
- `Continuation.kt` - Base continuation abstraction
- `HappyPathContinuation.kt` - Forward execution logic
- `RollbackPathContinuation.kt` - Compensating action execution

**Builder & Utilities**
- `SagaBuilder.kt` - Fluent API for saga construction
- `Sleep.kt` - Scheduling and delay functionality
- `TryFinally.kt` - Resource management patterns

**Context & Strategy**
- `CooperationContext.kt` - Shared data propagation
- `EventLoopStrategy.kt` - Execution policy framework
- `StandardEventLoopStrategy.kt` - Default execution policies

### Documentation Structure for Each Component

```kotlin
/**
 * [Component Name] - [Brief Purpose]
 * 
 * [1-2 sentences explaining what this component does and why it exists]
 * 
 * ## Structured Cooperation Role
 * [Explain how this component supports structured cooperation principles]
 * 
 * ## Key Concepts
 * [Define any domain-specific terms or concepts this component introduces]
 * 
 * ## Usage Patterns
 * [Show common ways this component is used, with simple examples]
 * 
 * ## Integration Points
 * [Describe how this component interacts with other parts of the system]
 * 
 * ## Important Behaviors
 * [Document edge cases, error scenarios, and subtle behaviors]
 * 
 * @see [Reference to blog posts, README sections, or related components]
 */
```

### Method and Property Documentation

For individual methods and properties:

```kotlin
/**
 * [Brief description of what this method does]
 * 
 * [More detailed explanation if the behavior is non-obvious or has important implications
 * for structured cooperation]
 * 
 * @param paramName [Description focusing on structured cooperation implications if relevant]
 * @return [Description of return value and its significance]
 * @throws ExceptionType [When and why this exception occurs in structured cooperation context]
 * 
 * TODO: [Address any relevant TODOs from the list above]
 */
```

## Code Architecture Overview

### Package Structure

**Blocking Implementation**
```
blocking/
├── coroutine/           # Core execution engine
│   ├── builder/         # Saga construction utilities
│   ├── continuation/    # Execution state management
│   └── structuredcooperation/ # Core protocol implementation
├── messaging/           # Message queue and routing
└── utils/               # Blocking-specific utilities
```

**Shared Components**
```
shared/
├── coroutine/
│   ├── context/         # Context propagation
│   ├── eventloop/       # Execution strategies
│   └── structuredcooperation/ # Common protocol types
├── messaging/           # Message abstractions
└── utils/               # Cross-implementation utilities
```

### Data Model

**Core Tables**
- `message` - Append-only message queue
- `message_events` - Structured cooperation state tracking

**Key Event Types**
- `EMITTED` - Message emission
- `SEEN` - Handler startup and locking
- `SUSPENDED` - Step completion
- `COMMITTED` - Saga completion
- `ROLLING_BACK` - Rollback initiation
- `ROLLBACK_EMITTED` - Rollback message emission
- `ROLLED_BACK` - Rollback completion
- `ROLLBACK_FAILED` - Rollback error
- `CANCELLATION_REQUESTED` - User-initiated cancellation

### Execution Flow

1. **Message Reception** - Handler subscribes to topic via PostgresMessageQueue
2. **State Fetching** - EventLoop queries pending coroutine states
3. **Continuation Building** - DistributedCoroutine creates executable continuation
4. **Step Execution** - Continuation runs next saga step
5. **State Persistence** - Results written to message_events table
6. **Message Emission** - Any emitted messages are committed transactionally
7. **Cooperation Checking** - System determines if dependent handlers can proceed

For detailed flow diagrams and examples, see the [README](README.md#important-components-and-high-level-flow).

## Essential Reading

### Primary Sources
1. **[Deprecating Eventual Consistency—Applying Principles of Structured Concurrency to Distributed Systems](https://developer.porn/posts/introducing-structured-cooperation/)** - Start here for foundational concepts
2. **[Towards an Implementation of Structured Cooperation](https://developer.porn/posts/implementing-structured-cooperation/)** - Technical implementation details, EventLoopStrategy, CooperationContext, and rollback mechanics
3. **[The Unreasonable Effectiveness of Structured Cooperation](https://developer.porn/posts/the-unreasonable-effectiveness-of-structured-cooperation/)** - Historical context and theoretical foundation explaining why structured cooperation works


### Project Documentation
- **[README.md](README.md)** - Comprehensive project overview with navigation guidance
- **[Glossary](README.md#glossary-of-terms)** - Definitions of all technical terms
- **[Component Guide](README.md#important-components-and-high-level-flow)** - Architecture overview with learning path

### Best Practices
- **Reference these sources extensively** rather than duplicating content
- **Link to specific sections** when discussing related concepts
- **Encourage progressive reading** - start with concepts, then dive into implementation
- **Provide context bridges** between blog theory and code reality

---

Remember: The goal is to help programmers of all backgrounds understand and learn from this implementation of structured cooperation. Every piece of documentation should serve the mission of making these powerful concepts accessible and practical.