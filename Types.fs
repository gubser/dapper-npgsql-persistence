namespace Infrastructure

open System.Collections.Generic

type QuerySingleRecord<'a> = string -> IReadOnlyDictionary<string, obj> -> Async<'a option>
type QueryManyRecords<'a> = string -> IReadOnlyDictionary<string, obj> -> Async<'a seq>

type CreateRecord<'a> = string -> 'a -> Async<unit>
type LoadRecord<'a> = string -> string -> obj -> Async<'a option>
type UpdateRecord<'a> = string -> string -> 'a -> Async<unit>
type DeleteRecord = string -> string -> obj -> Async<unit>
