# Testing Standards & Best Practices

## 1. Test Pyramid

* **Unit + Integration tests must represent ~90% of all tests.**
* The majority should be **unit tests**.
* Every module must have **at least one integration test**.

## 2. Prefer Classical Testing — No Mocks

Use **classical testing**, not mockist testing.

* **Do not use mocks under any circumstances**:

    * No Mockito or other mocking libraries.
    * No manually written mocks.
    * No alternative form of mocking.
* Test using **real objects and realistic scenarios**.
* Cover important, edge, happy, failure, and critical paths deliberately.
* A passing test should provide **high confidence that the tested behavior actually works**.

## 3. External Services & Containers

An **external service** is any service/module outside the module currently under test.

When tests require external services:

* Prefer containers where appropriate.
* Start each required container **once per module test suite**.
* Share it across tests using a static extension/configuration/namespace.
* Tests may reset or clean the service state between scenarios, but should **not repeatedly recreate the container**.
* Stop containers through the test suite/module shutdown lifecycle.
* Each infrastructure service should have a well-defined reusable **test extension/configuration class**.
* Use extension this  is very important . i should see the imported extension classes in the integration tests or any test that use the container.

## 4. Test Parallelism

Parallel execution is **not the first priority**.

Use parallel tests only when:

* It provides meaningful performance improvement.
* Tests remain isolated and deterministic.
* It does not introduce unnecessary complexity.
* There is a clear technical reason for enabling it.

## 5. Separate Pure Logic From I/O Tests

When possible, separate:

* **Pure unit tests** — real objects, deterministic inputs, no I/O.
* **I/O-dependent tests** — use containers or deterministic realistic test services.

Keep business/domain logic independently testable from infrastructure.

## 6. Test Independence

Tests within a class **must not depend on each other**.

Each test must independently establish its own required state and must produce the same result regardless of execution order.

## 7. Given–When–Then Structure

Test names and implementation should follow **Given–When–Then**.

Inside each test, comments are limited to exactly these three sections:

```java
// Given: valid traffic records

// When: aggregate traffic

// Then: totals match expected values
```

Rules:

* Keep each comment to one short line.
* Describe what that section actually does.
* Do not write comments that only say `Given`, `When`, or `Then`.
* **Do not add other comments inside tests.**

## 8. Infrastructure Test Configuration

Every infrastructure service used by the project should have a clean, reusable **test extension/configuration class**.

Avoid duplicating infrastructure setup across individual test classes.

## 9. Deterministic Test Data

**Do not use randomly generated test data or random population.**

Instead:

* Define deterministic, realistic scenarios.
* Explicitly cover happy, edge, critical, failure, and other important paths.
* Know exactly why every input exists.
* Calculate and explicitly define the expected result.
* Avoid redundant or meaningless scenarios.

Randomness does not increase meaningful domain coverage; deliberate scenarios do.

## 10. Never Use Direct `Thread.sleep()`

Do not use fixed sleeps such as:

```java
Thread.sleep(2000);
```

For asynchronous behavior, use an appropriate polling/awaiting library that:

* Checks the condition periodically, e.g. every `100 ms`.
* Continues immediately when the expected condition is satisfied.
* Has a strict maximum timeout, e.g. `5 seconds`.

## 11. Coverage Requirements

Maintain high test coverage across both:

* **Line coverage**
* **Branch coverage**

Coverage numbers alone are insufficient. Tests must validate meaningful behavior and scenarios.

## 12. Source Changes Require Test Changes

Whenever production (`src`) behavior changes:

* Review and update the corresponding tests.
* Do not modify tests merely to make them pass again.
* Add or update scenarios to verify the **new behavior, branches, edge cases, and affected paths**.

The goal is confidence in the changed behavior, not simply a green test suite.
