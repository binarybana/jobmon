using ZMQ
using JSON
using Zlib
using Nettle

ctx = Context(1)
s = Socket(ctx, ZMQ.REQ)
ZMQ.connect(s, "tcp://localhost:7001")
h = HashState(SHA1)
update!(h, "test")
jobhash = hexdigest!(h)
update!(h, "test2")
paramhash = hexdigest!(h)

fullhash = jobhash * "|" * paramhash
wiredata = bytestring(compress(json((Int=>Int)[1=>3]), 7))

#ZMQ.send(s, fullhash, SNDMORE)
#ZMQ.send(s, wiredata)
#ZMQ.recv(s)

#ZMQ.send(s, fullhash, SNDMORE)
#ZMQ.send(s, wiredata)
#ZMQ.recv(s)

#t1 = time()
for i=1:20
    tic()
    ZMQ.send(s, fullhash, SNDMORE)
    ZMQ.send(s, wiredata)
    msg = ZMQ.recv(s)
    #println(bytestring(msg))
    toc()
end
#println(time()-t1)
#println("^^ for 10000 messages")
