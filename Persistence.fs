module Infrastructure.Persistence

open System
open Npgsql
open Dapper

let connectionString = Environment.GetEnvironmentVariable "CONNECTION_STRING"

// From https://stackoverflow.com/questions/42797288/dapper-column-to-f-option-property
type OptionHandler<'T>() =
    inherit SqlMapper.TypeHandler<'T option>()

    override __.SetValue(param, value) = 
        let valueOrNull = 
            match value with
            | Some x -> box x
            | None -> null

        param.Value <- valueOrNull    

    override __.Parse value =
        if isNull value || value = box DBNull.Value 
        then None
        else Some (value :?> 'T)

type GeoJSONNetHandler<'T>() =
    inherit SqlMapper.TypeHandler<GeoJSON.Net.Geometry.Point>()

    override __.SetValue(param, value) = 
        param.Value <- value

    override __.Parse value =
        value :?> GeoJSON.Net.Geometry.Point

let setup () =
    NpgsqlConnection.GlobalTypeMapper.UseGeoJson(geographyAsDefault=true) |> ignore

    Dapper.DefaultTypeMap.MatchNamesWithUnderscores <- true
    SqlMapper.AddTypeHandler (OptionHandler<DateTime>())
    SqlMapper.AddTypeHandler (OptionHandler<string>())
    SqlMapper.AddTypeHandler (OptionHandler<byte[]>())
    SqlMapper.AddTypeHandler (OptionHandler<int>())
    SqlMapper.AddTypeHandler (OptionHandler<Guid>())
    SqlMapper.AddTypeHandler (GeoJSONNetHandler())

    ()

//
// query functions
let querySingleRecord<'a> : QuerySingleRecord<'a> = fun command param -> async {
    use conn = new NpgsqlConnection(connectionString)

    let! result = conn.QuerySingleOrDefaultAsync<'a>(command, param) |> Async.AwaitTask
    if isNull (box result) then
        return None
    else
        return Some result
}

let queryManyRecords<'a> : QueryManyRecords<'a> = fun command param -> async {
    use conn = new NpgsqlConnection(connectionString)

    return! conn.QueryAsync<'a>(command, param) |> Async.AwaitTask
}

// a list of properties of type 'a
let propertyNames<'a> =
    typedefof<'a>.GetProperties()
    |> Array.map (fun p -> p.Name)
    |> Array.sort

// creates a dictionary of (property name, property value)-pairs that can be passed to Dapper
let getRecordParams a =
    a.GetType().GetProperties() 
    |> Array.map (fun p -> p.Name, p.GetValue(a)) 
    |> dict

// convert a string of pascal case into snake case
let pascalToSnake pascalCaseName =
    pascalCaseName
    // any upper case character gets replaced by an underscore plus the lower case character
    |> Seq.mapi (fun i c ->
        if Char.IsUpper(c) then
            if i > 0 then 
                ['_'; Char.ToLower(c)] 
            else
                [Char.ToLower(c)]
        else
            [c]
    )
    |> Seq.concat
    |> Seq.toArray
    |> String


// read property names and convert them into snake case
let columnNames<'a> = 
    propertyNames<'a>
    |> Seq.map pascalToSnake
    |> Seq.sort
    |> Seq.toArray

//
// SQL command builders
let getInsertCommand<'a> tableName = 
    let columns = String.Join(", ", columnNames<'a>)
    let placeholders = String.Join(", ", propertyNames<'a> |> Array.map (fun c -> "@" + c))

    sprintf "INSERT INTO %s (%s) VALUES (%s)" tableName columns placeholders

let getSelectCommand tableName keyPropertyName =
    let keyColumnName = pascalToSnake keyPropertyName
    sprintf "SELECT * FROM %s WHERE %s = @%s" tableName keyColumnName keyPropertyName

let getUpdateCommand<'a> tableName keyPropertyName =
    let assignments =
        (columnNames<'a>, propertyNames<'a>)
        ||> Array.map2 (fun c p -> sprintf "%s = @%s" c p)

    let assignmentsString = String.Join(", ", assignments)

    let keyColumnName = pascalToSnake keyPropertyName

    sprintf "UPDATE %s SET %s WHERE %s = @%s" tableName assignmentsString keyColumnName keyPropertyName

let getDeleteCommand tableName keyPropertyName =
    let keyColumnName = pascalToSnake keyPropertyName
    sprintf "DELETE FROM %s WHERE %s = @%s" tableName keyColumnName keyPropertyName

//
// actual persistence functions
let createRecord<'a> : CreateRecord<'a> = fun tableName a -> async {
    use conn = new NpgsqlConnection(connectionString)

    let command = getInsertCommand<'a> tableName
    let param = getRecordParams a

    let! count = conn.ExecuteAsync(command, param) |> Async.AwaitTask
    if count = 0 then
        failwith "error creating record"
}

let loadRecord<'a> : LoadRecord<'a> = fun tableName keyPropertyName id -> async {
    use conn = new NpgsqlConnection(connectionString)

    let command = getSelectCommand tableName keyPropertyName
    let param = readOnlyDict [ keyPropertyName, id ]

    return! querySingleRecord<'a> command param
}

let updateRecord<'a> : UpdateRecord<'a> = fun tableName keyPropertyName a -> async {
    use conn = new NpgsqlConnection(connectionString)

    let command = getUpdateCommand<'a> tableName keyPropertyName
    let param = getRecordParams a

    let! count = conn.ExecuteAsync(command, param) |> Async.AwaitTask

    if count = 0 then
        failwith "error updating record"
}

let deleteRecord : DeleteRecord = fun tableName keyPropertyName id -> async {
    use conn = new NpgsqlConnection(connectionString)

    let command = getDeleteCommand tableName keyPropertyName
    let param = dict [ keyPropertyName, box id ]

    let! count = conn.ExecuteAsync(command, param) |> Async.AwaitTask
    if count = 0 then
        failwith "error deleting record"
}

