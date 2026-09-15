# Working agreement

Read SPEC.md first. It is the contract. Do not deviate from it without flagging.

## Hard constraints
- Limited-time total budget. Do not add abstractions, layers, or features not in SPEC.md.
- NestJS + pnpm. Use `amqp-connection-manager`, NOT Nest's built-in RMQ transport.
- Declare RabbitMQ topology explicitly in code. No hidden abstractions over ack,
  prefetch, or the retry chain.
- No database. In-memory repositories behind interfaces.
- No UI.

## Style
- Explain non-obvious decisions in a one-line comment. I have to defend every line.
- Small, focused commits.
- Do not generate barrel files, generic repositories, or CRUD scaffolding.
- Ask before introducing any dependency not already in package.json.