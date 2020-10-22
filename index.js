const avsc = require('avsc'),
      fs = require('fs'),
      fsPromises = fs.promises,
      through2 = require('through2'),
      util = require('util')

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

    console.log(new Date())
    for(let i=0;i<10000;i++){
        await appender.write(JSON.stringify({ _id: "abc", test: [1, 2, 3], strs: ["abc","def","ghk"] })+"\n")
        await appender.write(JSON.stringify({ operation: "delete" })+"\n")
    }
    console.log(new Date())

    //await encoder.write({ operation: "delete" })
    await appender.close()
}

async function m(){
    console.log("AVSC")
    await benchAvro()
    console.log("JSON")
    await benchJson()
}

m()