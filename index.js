const avsc = require('avsc'),
      fs = require('fs'),
      fsPromises = fs.promises,
      through2 = require('through2'),
      util = require('util'),
      zlib = require('zlib')

const testSchema = {
    type: 'record', // "Record" is Avro parlance for "structured object".
    name: 'test',
    fields:
        [
            { name: '_id', type: 'string' },
            { name: 'test', type: {type: "array", items: "int"} },
            { name: 'strs', type: {type: "array", items: "string"} },
        ]
}
const opSchema = {
    type: 'record', // "Record" is Avro parlance for "structured object".
    name: 'op',
    fields:
        [
            { name: 'operation', type: 'string' }
        ]
}

const registry = {}
const schema = [testSchema, opSchema]
const type = avsc.Type.forSchema(schema, {registry})


const encoder = new avsc.streams.BlockEncoder(type, {registry})

async function main(){
    const stream = fs.createWriteStream("test-file.avro", { defaultEncoding: 'binary' })
    var jamesbond = through2(function (chunk, encoding, callback) {
        console.log(chunk)
        this.push(chunk)
        return callback()
    })
    encoder.on('data', function(da){
        console.log({da})
    })

    encoder.pipe(jamesbond);
    jamesbond.pipe(stream)

    console.log('writing 1')
    await encoder.write({test: { _id: "abc", test: [1, 2, 3] }})
    console.log('written 1')
    console.log('writing 2')
    await encoder.write({op: { operation: "delete" }})
    console.log('written 2')
    //await encoder.write({ operation: "delete" })
    await encoder.end()
}



async function main_alt(){
    const opts = {writeFlushed: true}
    const appender = avsc.createFileAppender("append-file.avro", type, opts);

    console.log('writing 1')
    await appender.write({test: { _id: "abc", test: [1, 2, 3] }})
    console.log('written 1')
    console.log('writing 2')
    await appender.write({op: { operation: "delete" }})
    console.log('written 2')
    //await encoder.write({ operation: "delete" })
    await appender.end()
}



async function benchAvro(){
    const opts = {writeFlushed: true, blockSize:4096}
    fs.unlinkSync("append-file.avro")
    const appender = avsc.createFileAppender("append-file.avro", type, opts);

    console.log(new Date())
    const write = util.promisify(appender.write.bind(appender))
    for(let i=0;i<10000;i++){
        await write({test: { _id: "abc", test: [1, 2, 3], strs: ["abc","def","ghk"] }})
        await write({op: { operation: "delete" }})
    }
    console.log(new Date())

    //await encoder.write({ operation: "delete" })
    await appender.end()
}

async function benchJson(){
    const appender = await fsPromises.open('append-file.json', 'w')

    const sync = util.promisify(fs.fdatasync)
    console.log(new Date())
    for(let i=0;i<10000;i++){
        await appender.write(JSON.stringify({ _id: "abc", test: [1, 2, 3], strs: ["abc","def","ghk"] })+"\n")
        await sync(appender.fd)
        await appender.write(JSON.stringify({ operation: "delete" })+"\n")
        await sync(appender.fd)
    }
    console.log(new Date())

    //await encoder.write({ operation: "delete" })
    await appender.close()
}

async function benchBrotli(){
    const appender = fs.createWriteStream('append-file.brotli')

    const stream = zlib.createBrotliCompress({
        chunkSize: 1024,
        params: {
          [zlib.constants.BROTLI_PARAM_QUALITY]: 3
        }
    });
    stream.pipe(appender)

    const write = util.promisify(stream.write.bind(stream))
    const flush = util.promisify(stream.flush.bind(stream))
    const sync = util.promisify(fs.fdatasync)
    console.log(new Date())
    for(let i=0;i<10000;i++){
        await write(JSON.stringify({ _id: "abc", test: [1, 2, 3], strs: ["abc","def","ghk"] })+"\n")
        await flush()
        await sync(appender.fd)
        await write(JSON.stringify({ operation: "delete" })+"\n")
        await flush()
        await sync(appender.fd)
    }
    console.log(new Date())

    //await encoder.write({ operation: "delete" })
    await flush()
    //await appender.close()
}
const schemas = {}

/*const {sjs, attr} = require('slow-json-stringify')
async function writeJsons(stream, object, schemaName, schema){
    let s = schemas[schemaName]
    if(!s){
        await stream.write(`${schema}\r${schemaName}\r${JSON.stringify(schema)}`)
        let schemaParsed = {}
        for(let k in schema){
            let v = schema[k]
            if(Array.isArray(v)){
                schemaParsed[k] = array(v[0])
            }else{
                schemaParsed[k] = attr(v)
            }
        }
        s = schemas[schemaName] = sjs(schemaParsed)
    }

    await stream.write(`${schemaName}\r${s(object)}\n`)
}*/

const fjs = require('fast-json-stringify')
async function writeJsons(stream, object, schemaName, schema){
    let s = schemas[schemaName]
    if(!s){
        await stream.write(`${schema}\r${schemaName}\r${JSON.stringify(schema)}`)
        s = schemas[schemaName] = fjs(schema)
    }

    await stream.write(`${schemaName}\r${s(object)}\n`)
}


async function benchJsons(){
    const appender = await fsPromises.open('append-file.json', 'w')

    //const SchemaA = {_id: 'string', test: 'string[]', strs: 'string[]'}//['number']
    //const SchemaB = {operation: 'string'}
    const SchemaA = {
        type: 'object',
        properties: {
          _id: {
            type: 'string'
          },
          test: {
            type: 'array',
            items: {
                type: 'number'
            }
          },
          strs: {
            type: 'array',
            items: {
                type: 'string'
            }
          }
        }
    }
    const SchemaB = {
        type: 'object',
        properties: {
          operation: {
            type: 'string'
          }
        }
    }
    const sync = util.promisify(fs.fdatasync)
    console.log(new Date())
    for(let i=0;i<10000;i++){
        await writeJsons(appender, { _id: "abc", test: [1, 2, 3], strs: ["abc","def","ghk"] }, 'a', SchemaA)
        await sync(appender.fd)
        await writeJsons(appender, { operation: "delete" }, 'b', SchemaB)
        await sync(appender.fd)
    }
    console.log(new Date())

    //await encoder.write({ operation: "delete" })
    await appender.close()
}


async function m(){
    //console.log("AVSC")
    //await benchAvro()
    console.log("JSONS")
    await benchJsons()
    console.log("JSON")
    await benchJson()
    console.log("BROTLI")
    await benchBrotli()
}

m()