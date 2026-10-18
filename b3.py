import struct
M=0xFFFFFFFF
IV=[0x6A09E667,0xBB67AE85,0x3C6EF372,0xA54FF53A,0x510E527F,0x9B05688C,0x1F83D9AB,0x5BE0CD19]
PERM=[2,6,3,10,7,0,4,13,1,11,12,5,9,14,15,8]
CS,CE,PA,RO,XOF=1,2,4,8,16
def rotr(x,n): return ((x>>n)|(x<<(32-n)))&M
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
def compress(cv,w16,ctr,flags,blen):
    """w16: 16 message words. state=[cv(8), w0..w7] then [12..16)=(ctr_lo,ctr_hi,flags,blen)."""
    st=list(cv)+list(w16[:8])
    st[12]=ctr&M; st[13]=(ctr>>32)&M; st[14]=flags; st[15]=blen
    m=list(w16[:10])+[ctr&M,(ctr>>32)&M,flags,blen]
    m=m+[m[0],m[1]]
    order=list(range(16))
    for r in range(7):
        mm=[m[order[i]] for i in range(16)]
        rnd(st,mm)
        order=[PERM[order[i]] for i in range(16)]
    return st
def chaining(st,cv):
    out=[0]*8
    for i in range(4):
        out[i]=(st[i]+st[i+8])&M
        out[i+4]=(st[i+4]+cv[i])&M
    return out
def wle(b):
    b=bytes(b)
    if len(b)<64: b=b+bytes(64-len(b))
    return list(struct.unpack('<16I',b))
def output_node(data,key,counter,is_root):
    if len(data)<=1024:
        cv=list(key) if counter==0 else list(IV)
        n=len(data);c=counter;pos=0
        while True:
            rem=n-pos;take=min(64,max(0,rem));blk=data[pos:pos+take]
            last=rem<=64
            f=(CS if pos==0 else 0)|((CE|(RO if is_root else 0)) if last else 0)
            st=compress(cv,wle(blk),c,f,take)
            cv=chaining(st,cv)
            if last: return cv,(st[8:16],cv,c,f,take)
            c+=1;pos+=take
    chunks=(len(data)+1023)//1024
    p=1
    while p*2<chunks:p*=2
    pb=min(p*1024,len(data))
    cvL,_=output_node(data[:pb],key,counter,False)
    cvR,_=output_node(data[pb:],key,counter+p,False)
    cv=[cvL[0],cvL[1],cvL[2],cvL[3],cvR[0],cvR[1],cvR[2],cvR[3]]
    w=cv[:8]+[0]*8
    f=PA|(RO if is_root else 0)
    st=compress(cv,w,0,f,64)
    chain=chaining(st,cv)
    return chain,(st[8:16],chain,0,f,64)
def root_output_words(o):
    sched16,cv,c,f,blen=o
    st=list(IV)+list(sched16[:6])+[c&M,(c>>32)&M,f,blen]
    # one pre-round compression with m = perm(sched16)? NO — reference uses the SAME
    # compress() layout but only ONE round and the block IS sched16 (16 words):
    st=list(IV)+list(sched16)
    m=list(sched16[:10])+[c&M,(c>>32)&M,f,blen]
    m=m+[m[0],m[1]]
    mm=[m[PERM[i]] for i in range(16)]
    rnd(st,mm)
    icv=[0]*8
    for i in range(4):
        icv[i]=(st[i]+st[i+8])&M
        icv[i+4]=(st[i+4]+st[i+12])&M
    out=[icv[i]^icv[i+4] for i in range(4)]
    return out
def hash256(data,key=None):
    _,o=output_node(bytes(data),key or IV,0,True)
    return struct.pack('<4I',*root_output_words(o))
if __name__=="__main__":
    import blake3 as bref
    ok=True
    for ln in [0,1,2,5,10,63,64,65,1023,1024,1025,2048,2080,3072,4096,5000,31774]:
        d=bytes(i%251 for i in range(ln))
        h=hash256(d).hex(); e=bref.blake3(d).hexdigest()
        good=h==e; ok=ok and good
        print(ln,"OK" if good else ("FAIL\n got "+h+"\n exp "+e))
    print("ALL OK" if ok else "BROKEN")
