package ag.dc.zhttp_cqrs.cqrs

import java.nio.charset.Charset
import java.time.Instant
import java.util.Random
import java.util.UUID
import scala.util.Random.apply
import scala.util.Try
import zhttp.http.*
import zhttp.http.HttpError.NotFound
import zhttp.http.Method
import zhttp.http.Path
import zhttp.http.Request
import zhttp.http.Response
import zhttp.http.URL
import zhttp.logging.Logger
import zio.Chunk
import zio.Ref
import zio.ZIO
import zio.json.*


type AggregateIdType = UUID | Int

type AggregateState = "Open" | "Closed"


enum CommonHeaders(name: String):
  def getName = this.name
  case CorrelationId extends CommonHeaders("X-Correlation-Id")
  case SessionId extends CommonHeaders("X-Session-Id")


trait CreationTime:
  val createdAt: Instant

trait CreationUserName:
  val createdByUserName: String

trait ModificationTime:
  val lastModifiedAt: Instant

trait ModificationUserName:
  val lastModifiedByUserName: String

trait FullMetadata
    extends CreationTime
    with CreationUserName
    with ModificationTime
    with ModificationUserName

trait Aggregate extends CreationTime with ModificationTime with Product with Serializable:
  type IdType <: AggregateIdType
  val id: IdType
  val state: AggregateState = "Open"

case class RequestError(status: Status, message: String) extends Throwable:
  def toResponse(cs: Charset = Charset.defaultCharset()): Response =
    Response(status, Headers(), HttpData.fromString(message, cs))

object RequestError:
  def apply(status: Status, message: String): RequestError =
    new RequestError(status, message)

abstract trait Message[D1 <: Serializable, D2 <: Serializable]
    extends Product
    with Serializable:
  val messageId: UUID
  val correlationId: UUID
  val createdAt: Instant
  val issuer: String
  val messageData: D1
  val metadata: Map[String, D2]

given msgOrd[T <: Message[?, ?]]: Ordering[T] with
  override def compare(x: T, y: T): Int =
    x.createdAt.compareTo(y.createdAt)

abstract trait IncomingMessage[D1 <: Serializable, D2 <: Serializable]
    extends Message[D1, D2]:
  val targetLocator: String | (Method, URL)
  val sourceIdentifier: String

// Implementations should be configuratble to include headerData or not
sealed abstract trait IncomingHTTPMessage[
    D1 <: Serializable,
    D2 <: Serializable
] extends Message[D1, D2]:
  val targetLocator: (Method, URL)
  val headerData: Option[zhttp.http.Headers]

trait Command[D1 <: Serializable, D2 <: Serializable]
    extends IncomingHTTPMessage[D1, D2]
trait Query[D1 <: Serializable, D2 <: Serializable]
    extends IncomingHTTPMessage[D1, D2]

trait CommandFactory:
  val requestToCommand: PartialFunction[
    Request,
    ZIO[Any, Throwable, Either[RequestError, Command[?, ?]]]
  ]
  def registerCommands[C <: Command[?, ?]](
      pfs: List[
        PartialFunction[Request, ZIO[Any, Throwable, Either[RequestError, C]]]
      ]
  ): CommandFactory

trait QueryFactory:
  val requestToQuery: PartialFunction[
    Request,
    ZIO[Any, Throwable, Either[RequestError, Query[?, ?]]]
  ]
  def registerQueries[C <: Query[?, ?]](
      pfs: List[
        PartialFunction[Request, ZIO[Any, Throwable, Either[RequestError, C]]]
      ]
  ): QueryFactory

class GenericCommandFactory(
    val registeredCommands: List[PartialFunction[
      Request,
      ZIO[Any, Throwable, Either[RequestError, Command[?, ?]]]
    ]] = List()
) extends CommandFactory:
  // OR the individual PartialFunctions, then lift to a total function to Option
  val composedFactories = registeredCommands.reduce((a, b) => a.orElse(b))
  def registerCommands[C <: Command[?, ?]](
      pfs: List[
        PartialFunction[Request, ZIO[Any, Throwable, Either[RequestError, C]]]
      ]
  ): GenericCommandFactory =
    new GenericCommandFactory(registeredCommands.appendedAll(pfs))
  val requestToCommand: PartialFunction[Request, ZIO[Any, Throwable, Either[
    RequestError,
    Command[?, ?]
  ]]] = composedFactories

class GenericQueryFactory(
    val registeredQueries: List[PartialFunction[
      Request,
      ZIO[Any, Throwable, Either[RequestError, Query[?, ?]]]
    ]] = List()
) extends QueryFactory:
  val composedFactories = registeredQueries.reduce((a, b) => a.orElse(b))
  def registerQueries[Q <: Query[?, ?]](
      pfs: List[
        PartialFunction[Request, ZIO[Any, Throwable, Either[RequestError, Q]]]
      ]
  ): GenericQueryFactory =
    new GenericQueryFactory(registeredQueries.appendedAll(pfs))
  val requestToQuery: PartialFunction[Request, ZIO[Any, Throwable, Either[
    RequestError,
    Query[?, ?]
  ]]] = composedFactories

trait IncomingHTTPMessageFactory:
  val queryFactory: QueryFactory
  val commandFactory: CommandFactory
  val notFoundError = ZIO.succeed(Left(RequestError(Status.NotFound, "")))
  val requestToMessage: PartialFunction[Request, ZIO[Any, Throwable, Either[
    RequestError,
    IncomingHTTPMessage[?, ?]
  ]]] = {
    case r: Request if (r.method.equals(Method.GET)) =>
      queryFactory.requestToQuery(r)
    case r: Request if (r.method.equals(Method.PUT)) =>
      commandFactory.requestToCommand(r)
    case r: Request if (r.method.equals(Method.PATCH)) =>
      commandFactory.requestToCommand(r)
    case r: Request if (r.method.equals(Method.POST)) =>
      commandFactory.requestToCommand(r)
    case r: Request => {
      Console.println(s"No matching message found for method")
      ZIO.fail(NotFound(r.path))
    }
  }

class GenericIncomingHTTPMessageFactory(
    val queryFactory: QueryFactory,
    val commandFactory: CommandFactory
) extends IncomingHTTPMessageFactory

sealed trait IncomingHTTPMessageHandler[
    MessageTypeUnion <: (Query[?, ?] | Command[?, ?]),
    R <: Serializable
]:
  def handles[T <: IncomingHTTPMessage[?, ?]](message: T): Boolean
  def handleMessageZIO(message: MessageTypeUnion): ZIO[Any, Throwable, R]

trait CommandHandler[CommandTypeUnion <: Command[?, ?], R <: Serializable]
    extends IncomingHTTPMessageHandler[CommandTypeUnion, R]:
  type HandledCommands = CommandTypeUnion
  def handles[T <: IncomingHTTPMessage[?, ?]](message: T): Boolean =
    message match {
      case _: HandledCommands => true
      case _                  => false
    }
  def handleCommandZIO(command: CommandTypeUnion): ZIO[Any, Throwable, R]
  def handleMessageZIO(message: CommandTypeUnion): ZIO[Any, Throwable, R] =
    handleCommandZIO(message)

trait QueryHandler[QueryTypeUnion <: Query[?, ?], R <: Serializable]
    extends IncomingHTTPMessageHandler[QueryTypeUnion, R]:
  type HandledQueries = QueryTypeUnion
  def handles[T <: IncomingHTTPMessage[?, ?]](message: T): Boolean =
    message match {
      case _: HandledQueries => true
      case _                 => false
    }
  def handleQueryZIO(query: QueryTypeUnion): ZIO[Any, Throwable, R]
  def handleMessageZIO(message: QueryTypeUnion): ZIO[Any, Throwable, R] =
    handleQueryZIO(message)

trait IncomingHTTPMessageDispatcher[R <: Serializable](
    protected val queryHandlers: List[QueryHandler[?, R]],
    protected val commandHandlers: List[CommandHandler[?, R]],
    protected val defaultNotFound: R,
    protected val reqErrorToR: RequestError => R
):
  def handles(
      message: Query[?, ?] | Command[?, ?]
  ): Boolean =
    message match {
      case q: Query[?, ?]   => queryHandlers.exists(_.handles(q))
      case c: Command[?, ?] => commandHandlers.exists(_.handles(c))
    }
  def dispatchZIO(message: Query[?, ?] | Command[?, ?]): ZIO[Any, Throwable, R] =
    Console.println(
      s"Dispatching message of type ${message.getClass.getSimpleName()}."
    )
    val opt = message match {
      case q: Query[?, ?] =>
        queryHandlers
          .find(_.handles(q))
          .map(h => h.handleQueryZIO(q.asInstanceOf[h.HandledQueries]))
      case c: Command[?, ?] =>
        commandHandlers
          .find(_.handles(c))
          .map(h => h.handleCommandZIO(c.asInstanceOf[h.HandledCommands]))
    }
    opt match {
      case Some(z) => {
        Console.println(s"Found handler for message. Returning handler-ZIO.")
        z
      }
      case None => {
        Console.println(
          s"No handler found for message with UUID ${message.messageId.toString()} " +
          s"and correlation-Id ${message.correlationId.toString()}. " +
          "Proceeding with default not found handling."
        )
        ZIO.succeed(defaultNotFound)
      }
    }

class GenericIncomingHTTPMessageDispatcher(
  queryHandlers: List[QueryHandler[?, Response]] = List(),
  commandHandlers: List[CommandHandler[?, Response]] = List(),
  defaultNotFound: Response = Response.status(Status.NotFound),
  reqErrorToR: RequestError => Response = (r: RequestError) => r.toResponse()
)
  extends IncomingHTTPMessageDispatcher[Response](
    queryHandlers,
    commandHandlers,
    defaultNotFound,
    reqErrorToR
  ):
  def registerHandler(
      handler: CommandHandler[?, Response] | QueryHandler[?, Response]
  ): GenericIncomingHTTPMessageDispatcher =
    handler match {
      case qh: QueryHandler[?, Response] =>
        new GenericIncomingHTTPMessageDispatcher(
          queryHandlers.appended(qh),
          commandHandlers,
          defaultNotFound
        )
      case ch: CommandHandler[?, Response] =>
        new GenericIncomingHTTPMessageDispatcher(
          queryHandlers,
          commandHandlers.appended(ch),
          defaultNotFound
        )
    }

abstract trait OutgoingMessage[D1 <: Serializable, D2 <: Serializable]
    extends Message[D1, D2]:
  val sourceIdentifier: String

// Implementations should be configurable to include propagationAncestors or not
trait Event[D1 <: Serializable, D2 <: Serializable]
    extends OutgoingMessage[D1, D2]:
  val propagationAncestors: Option[List[(String, Instant)]]
  val prohibitedPropagationTargets: Option[List[String] | "All"]

trait DomainEvent[D1 <: Serializable, D2 <: Serializable, A <: Aggregate]
    extends Event[D1, D2]:
  type AggregateType = A
  val isInitialEvent = false
  val aggregateLocator: (String, AggregateIdType)
  def applyPatch(aggregate: A): A

abstract class GenericDomainEvent[
    D1 <: Serializable,
    D2 <: Serializable,
    C <: Aggregate
] extends DomainEvent[D1, D2, C]:
  val messageId: UUID
  val correlationId: UUID
  val createdAt: Instant
  val issuer: String
  val messageData: D1
  val metadata: Map[String, D2]
  val aggregateLocator: (String, AggregateIdType)
  val sourceIdentifier: String

trait InitialDomainEvent[D1 <: Serializable, D2 <: Serializable, A <: Aggregate]
    extends DomainEvent[D1, D2, A]:
  override val isInitialEvent = true

abstract class GenericInitialDomainEvent[
    D1 <: Serializable,
    D2 <: Serializable,
    C <: Aggregate
] extends InitialDomainEvent[D1, D2, C]:
  val messageId: UUID
  val correlationId: UUID
  val createdAt: Instant
  val issuer: String
  val messageData: D1
  val metadata: Map[String, D2]
  val aggregateLocator: (String, AggregateIdType)
  val sourceIdentifier: String

trait IntegrationEvent[D1 <: Serializable, D2 <: Serializable]
    extends Event[D1, D2]

class DomainEventProjector[C <: Aggregate]:
  type TargetAggregate = C

  def projectEvents(
      baseAggregate: TargetAggregate,
      events: List[DomainEvent[?, ?, TargetAggregate]]
  ): TargetAggregate =

    val evtsAfter = baseAggregate.lastModifiedAt
    val filteredAndSorted =
      events
        .filter(_.createdAt.isAfter(evtsAfter))
        .sorted

    filteredAndSorted.foldLeft(baseAggregate)((a, e) => e.applyPatch(a))

  def projectEvents[
      D1 <: Serializable,
      M1 <: Serializable,
      C <: Aggregate,
      D2 <: Serializable,
      M2 <: Serializable
  ](
      generator: InitialDomainEvent[D1, M1, TargetAggregate] => TargetAggregate,
      events: (
          InitialDomainEvent[D1, M1, TargetAggregate],
          List[DomainEvent[D2, M2, TargetAggregate]]
      )
  ): TargetAggregate =

    val initialEvent = events._1
    val otherEvents = events._2
    val filteredAndSorted =
      otherEvents
        .filter(e =>
          !e.isInitialEvent && e.createdAt.isAfter(initialEvent.createdAt)
        )
        .sorted

    val initialAggregate = generator(initialEvent)
    filteredAndSorted.foldLeft(initialAggregate)((a, e) => e.applyPatch(a))

object DomainEventProjector:
  def apply[C <: Aggregate]() = new DomainEventProjector[C]

trait BlockingEventHandler
trait CPUHeavyEventHandler

trait EventHandler[E <: Event[?, ?]]:
  // List of type/class-names of events that this handler can handle
  val handles: List[Class[E]]
  def handle(event: E): ZIO[Any, Throwable, Any]

trait DomainEventHandler[C <: Aggregate] extends EventHandler[DomainEvent[?, ?, C]]:
  type TargetAggregate = C
  override def handle(event: DomainEvent[?, ?, C]): ZIO[Any, Throwable, Any]


object SynchronousDomainEventHook:
  val registeredHandlers: scala.collection.mutable.Map[String, List[DomainEventHandler[?]]] = 
    scala.collection.mutable.Map()
  def registerHandler(handler: DomainEventHandler[?]): Unit = {
    handler.handles.foreach(evtClass => {
      val existingHandlers = registeredHandlers.getOrElse(evtClass.getName, List())
      val appendedHandlers = existingHandlers.appended(handler)
      registeredHandlers(evtClass.getName) = appendedHandlers
    })
  }
  def raiseEvent(event: DomainEvent[?, ?, ?]): ZIO[Any, Throwable, Any] = {
    registeredHandlers get event.getClass.getName match {
      case Some(list: List[DomainEventHandler[?]]) => 
        list
          .map(h => h.handle(event.asInstanceOf[DomainEvent[?, ?, h.TargetAggregate]]))
          .reduce(_ *> _)
      case None => ZIO.succeed(())
    }
  }

object AsynchronousDomainEventHook:
  val registeredHandlers: scala.collection.mutable.Map[String, List[DomainEventHandler[?]]] = 
    scala.collection.mutable.Map()
  def registerHandler(handler: DomainEventHandler[?]): Unit = {
    handler.handles.foreach(evtClass => {
      val existingHandlers = registeredHandlers.getOrElse(evtClass.getName, List())
      val appendedHandlers = existingHandlers.appended(handler)
      registeredHandlers(evtClass.getName) = appendedHandlers
    })
  }
  // Call handle.handle concurrently for each event
  def raiseEvent(event: DomainEvent[?, ?, ?]): ZIO[Any, Throwable, Any] = {
    // @TODO: Improve by changing map keys to type Class[?], filtering keys by filterKeys(k => k.isAssignableFrom(event.getClass))
    registeredHandlers get event.getClass.getName match {
      case Some(list: List[DomainEventHandler[?]])  => 
        list
          .map(handler => 
            if (handler.isInstanceOf[BlockingEventHandler] || handler.isInstanceOf[CPUHeavyEventHandler])
            then 
              ZIO.attemptBlocking { handler.handle(event.asInstanceOf[DomainEvent[?, ?, handler.TargetAggregate]]) }              
            else 
              handler.handle(event.asInstanceOf[DomainEvent[?, ?, handler.TargetAggregate]])
          )
          .map(_.fork)
          // @TODO: Do we need to join fibres? We need not await their completion here
          //.map(_.join)
          .reduce(_ *> _)
      case None => ZIO.succeed(())
    }
  }
