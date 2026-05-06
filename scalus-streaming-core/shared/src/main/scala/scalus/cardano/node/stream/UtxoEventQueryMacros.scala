package scalus.cardano.node.stream

import scala.quoted.*
import scalus.cardano.ledger.Utxo
import scalus.cardano.node.UtxoQueryMacros

/** HOAS macro DSL for [[UtxoEventQuery]].
  *
  * A subscriber declares both the UTxO predicate (which UTxOs to observe) and the event-kind
  * predicate (which lifecycle events to receive) in one lambda:
  *
  * {{{
  *   import scalus.cardano.node.stream.UtxoEventQueryMacros.buildEventQuery
  *
  *   val q = buildEventQuery { (u, kind) =>
  *       u.output.address == myAddress && kind == UtxoEventType.Created
  *   }
  * }}}
  *
  * The macro splits the lambda body at top-level `&&` into a *utxo-side* (references only `u`) and
  * a *kind-side* (references only `kind`). The utxo-side is delegated to the upstream
  * [[scalus.cardano.node.UtxoQueryMacros.buildQuery]] — same patterns as that macro accepts — by
  * synthesising a fresh `Utxo => Boolean` lambda. The kind-side is parsed locally; the result is
  * folded into [[UtxoEventQuery.types]].
  *
  * ==Supported kind-side patterns==
  *   - `kind == UtxoEventType.X` (or reversed `UtxoEventType.X == kind`)
  *   - `kind != UtxoEventType.X`
  *   - `(kind == X) || (kind == Y)` (OR-chain of equalities)
  *
  * ==Lambda shape==
  *   - `(u, kind) => utxoBody` — kind-side absent ⇒ all event types subscribed.
  *   - `(u, kind) => kindBody` — utxo-side absent ⇒ rejected (an event query needs a source).
  *   - `(u, kind) => utxoBody && kindBody` — the common case.
  *   - Each leaf in the top-level `&&` chain must reference *only* `u` or *only* `kind`; mixing
  *     fails at compile time.
  */
object UtxoEventQueryMacros {

    inline def buildEventQuery(
        inline f: (Utxo, UtxoEventType) => Boolean
    ): UtxoEventQuery = ${ buildEventQueryImpl('f) }

    private def buildEventQueryImpl(
        f: Expr[(Utxo, UtxoEventType) => Boolean]
    )(using Quotes): Expr[UtxoEventQuery] = {
        import quotes.reflect.*

        @scala.annotation.tailrec
        def peel(t: Term): Term = t match
            case Inlined(_, Nil, inner) => peel(inner)
            case Block(Nil, inner)      => peel(inner)
            case Typed(inner, _)        => peel(inner)
            case _                      => t

        def extractLambda(t: Term): (Symbol, Symbol, Term) = peel(t) match
            case Lambda(List(p1, p2), body) => (p1.symbol, p2.symbol, body)
            case other =>
                report.errorAndAbort(
                  s"expected a `(Utxo, UtxoEventType) => Boolean` lambda, got: ${other.show}",
                  other.pos
                )

        val (uParam, kindParam, body) = extractLambda(f.asTerm)

        def collectAnd(t: Term): List[Term] = peel(t) match
            case Apply(Select(lhs, "&&"), List(rhs)) => collectAnd(lhs) ::: collectAnd(rhs)
            case other                               => List(other)

        def references(term: Term, target: Symbol): Boolean = {
            var found = false
            val tr = new TreeTraverser {
                override def traverseTree(tree: Tree)(owner: Symbol): Unit = tree match
                    case Ident(_) if tree.symbol == target => found = true
                    case _                                 => super.traverseTree(tree)(owner)
            }
            tr.traverseTree(term)(Symbol.spliceOwner)
            found
        }

        // --- Step 1: classify each top-level && leaf as utxo-side or kind-side. ---

        val leaves = collectAnd(body)

        val (uLeaves, kindLeaves) = leaves.foldRight(
          (List.empty[Term], List.empty[Term])
        ) { case (leaf, (us, ks)) =>
            (references(leaf, uParam), references(leaf, kindParam)) match
                case (true, false) => (leaf :: us, ks)
                case (false, true) => (us, leaf :: ks)
                case (true, true) =>
                    report.errorAndAbort(
                      "event-query leaf references both `u` and `kind` — split it into two " +
                          "leaves joined by `&&` so each side mentions a single parameter.",
                      leaf.pos
                    )
                case (false, false) =>
                    report.errorAndAbort(
                      "event-query leaf references neither `u` nor `kind`. Drop it or move the " +
                          "constant outside the lambda.",
                      leaf.pos
                    )
        }

        if uLeaves.isEmpty then
            report.errorAndAbort(
              "event query needs at least one `u`-side predicate (a source); " +
                  "kind alone is not enough.",
              body.pos
            )

        // --- Step 2: lower the utxo side via the upstream macro. ---

        val utxoBody: Term = uLeaves.reduceLeft { (l, r) =>
            Apply(Select.unique(l, "&&"), List(r))
        }

        // Rebind references to the outer `u` parameter so they resolve to the freshly-introduced
        // single-arg lambda's own parameter symbol. Without this rewrite the synthesised lambda
        // would still close over the now-defunct outer parameter and fail the typer.
        val utxoTpe = TypeRepr.of[Utxo]
        val mtpe = MethodType(List("u"))(_ => List(utxoTpe), _ => TypeRepr.of[Boolean])
        val utxoLambda: Term = Lambda(
          Symbol.spliceOwner,
          mtpe,
          { (lamSym, args) =>
              val newU = args.head.asInstanceOf[Term]
              new TreeMap {
                  override def transformTerm(tree: Term)(owner: Symbol): Term = tree match
                      case Ident(_) if tree.symbol == uParam => newU
                      case other                             => super.transformTerm(other)(owner)
              }.transformTerm(utxoBody)(lamSym)
          }
        )
        val utxoLambdaExpr = utxoLambda.asExprOf[Utxo => Boolean]
        val utxoQueryExpr = '{ UtxoQueryMacros.buildQuery($utxoLambdaExpr) }

        // --- Step 3: parse the kind side into Set[UtxoEventType]. ---

        def parseKindLeaf(leaf: Term): Expr[Set[UtxoEventType]] = {
            def isKindIdent(term: Term): Boolean = peel(term) match
                case ident: Ident => ident.symbol == kindParam
                case _            => false

            def kindLiteral(term: Term): Option[Expr[UtxoEventType]] =
                if references(term, kindParam) then None
                else Some(term.asExprOf[UtxoEventType])

            peel(leaf) match
                // OR-chain: split and union
                case Apply(Select(l, "||"), List(r)) =>
                    val ls = parseKindLeaf(l)
                    val rs = parseKindLeaf(r)
                    '{ $ls.union($rs) }

                // kind == X  (or X == kind)
                case Apply(Select(lhs, "=="), List(rhs))
                    if isKindIdent(lhs) && kindLiteral(rhs).isDefined =>
                    val k = kindLiteral(rhs).get
                    '{ Set($k) }
                case Apply(Select(lhs, "=="), List(rhs))
                    if isKindIdent(rhs) && kindLiteral(lhs).isDefined =>
                    val k = kindLiteral(lhs).get
                    '{ Set($k) }

                // kind != X  (or X != kind)
                case Apply(Select(lhs, "!="), List(rhs))
                    if isKindIdent(lhs) && kindLiteral(rhs).isDefined =>
                    val k = kindLiteral(rhs).get
                    '{ UtxoEventType.all - $k }
                case Apply(Select(lhs, "!="), List(rhs))
                    if isKindIdent(rhs) && kindLiteral(lhs).isDefined =>
                    val k = kindLiteral(lhs).get
                    '{ UtxoEventType.all - $k }

                case other =>
                    report.errorAndAbort(
                      s"unsupported kind-side shape: ${other.show}.\n" +
                          "Supported: `kind == UtxoEventType.X`, `kind != UtxoEventType.X`, " +
                          "and `||`-chains of `kind == X`.",
                      other.pos
                    )
        }

        // Multiple `&&`-chained kind leaves narrow the type set — intersect them.
        val typesExpr: Expr[Set[UtxoEventType]] =
            if kindLeaves.isEmpty then '{ UtxoEventType.all }
            else kindLeaves.map(parseKindLeaf).reduceLeft((a, b) => '{ $a.intersect($b) })

        '{ UtxoEventQuery($utxoQueryExpr, $typesExpr) }
    }
}
