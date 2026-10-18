import struct
M = 0xFFFFFFFF
IV = [0x6A09E667,0xBB67AE85,0x3C6EF372,0xA54FF53A,0x510E527F,0x9B05688C,0x1F83D9AB,0x5BE0CD19]
PERM = [2,6,3,10,7,0,4,13,1,11,12,5,9,14,15,8]
CHUNK_START,CHUNK_END,PARENT,ROOT,XOF = 1,2,4,8,16

def rotr(x,n): return ((x>>n)|(x<<(32-n))) & M

def g(s,a,b,c,d,mx,my):
    s[a]=(s[a]+s[b]+mx)&M; s[d]=rotr(s[d]^s[a],16)
    s[c]=(s[c]+s[d])&M;   s[b]=rotr(s[b]^s[c],12)
    s[a]=(s[a]+s[b]+my)&M; s[d]=rotr(s[d]^s[a],8)
    s[c]=(s[c]+s[d])&M;   s[b]=rotr(s[b]^s[c],7)

def rnd(s,m):
    g(s,0,4,8,12,m[0],m[1]); g(s,1,5,9,13,m[2],m[3])
    g(s,2,6,10,14,m[4],m[5]); g(s,3,7,11,15,m[6],m[7])
    g(s,0,5,10,15,m[8],m[9]); g(s,1,6,11,12,m[10],m[11])
    g(s,2,7,8,13,m[12],m[13]); g(s,3,4,9,14,m[14],m[15])

def compress_full(cv, iw, ctr, flags, blen):
    """iw: up to 10 words. m=[iw(10),ctr_lo,ctr_hi,flags,blen]; m[14]=m[0];m[15]=m[1]."""
    st=list(cv)+list(iw[:6])+[ctr&M,(ctr>>32)&M,flags,blen]
    m=list(iw)+[ctr&M,(ctr>>32)&M,flags,blen]
    m=m+[m[0],m[1]]  # len 16
    order=list(range(16))
    for r in range(7):
        mm=[m[order[i]] for i in range(16)]
        rnd(st,mm)
        order=[PERM[order[i]] for i in range(16)]
    return st

def chaining(st, cv):
    out=[0]*8
    for i in range(4):
        out[i]=(st[i]+st[i+8])&M
        out[i+4]=(st[i+4]+cv[i])&M
    return out

def wle(b):
    b=b+bytes((-len(b))%4) if False else b
    return list(struct.unpack('<16I', bytes(b).ljust(64,b'\0')[:64]))

def output_node(data, key, counter, is_root):
    if len(data)<=1024:
        cv=list(key) if counter==0 else list(IV)
        n=len(data); c=counter; pos=0
        while True:
            rem=n-pos
            take=min(64,max(0,rem))
            blk=data[pos:pos+take]
            last=rem<=64
            f=(CHUNK_START if pos==0 else 0)|((CHUNK_END|(ROOT if is_root else 0)) if last else 0)
            st=compress_full(cv,wle(blk),c,f,take)
            cv=chaining(st,cv)
            if last:
                return cv,(st[8:16],cv,c,f,take)
            c+=1; pos+=take
    chunks=(len(data)+1023)//1024
    p=1
    while p*2<chunks: p*=2
    pb=min(p*1024,len(data))
    cvL,_=output_node(data[:pb],key,counter,False)
    cvR,_=output_node(data[pb:],key,counter+p,False)
    cv=[cvL[0],cvL[1],cvL[2],cvL[3],cvR[0],cvR[1],cvR[2],cvR[3]]
    iw=[cvL[0],cvL[1],cvL[2],cvL[3],cvR[0],cvR[1],cvR[2],cvR[3]]
    f=PARENT|(ROOT if is_root else 0)
    st=compress_full(cv,iw,0,f,64)
    chain=chaining(st,cv)
    return chain,(st[8:16],chain,0,f,64)

def root_output_bytes(o):
    iw10, cv, ctr, flags, blen = o
    st=list(IV)+list(iw10)+[ctr&M,(ctr>>32)&M,flags,blen]
    m=list(iw10)+[ctr&M,(ctr>>32)&M,flags,blen]
    m=m+[m[0],m[1]]
    mm=[m[PERM[i]] for i in range(16)]
    rnd(st,mm)
    icv=[0]*8
    for i in range(4):
        icv[i]=(st[i]+st[i+8])&M
        icv[i+4]=(st[i+4]+st[i+12])&M
    out=[icv[i]^icv[i+4] for i in range(4)]
    return struct.pack('<8I',*out)

def hash256(data, key=None):
    _,o=output_node(bytes(data), key or IV, 0, True)
    return root_output_bytes(o)[:32]

if __name__=="__main__":
    vecs={0:"af1349b9f5f9a1a6a0404dea36dcc94a5157b7294da91de",
          1:"2d3adedff11b61f14c886e35afa63caf01c65a8a5d3538f7",
          1024:"42214739f095a406f3fc83de48825614616054a58152f7b2"}
    for ln in [0,1,2,5,10]:
        d=bytes(i%251 for i in range(ln))
        print(ln, hash256(d).hex()[:24])
