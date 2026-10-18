import struct
M=0xFFFFFFFF
IV=[0x6A09E667,0xBB67AE85,0x3C6EF372,0xA54FF53A,0x510E527F,0x9B05688C,0x1F83D9AB,0x5BE0CD19]
PERM=[2,6,3,10,7,0,4,13,1,11,12,5,9,14,15,8]
CS,CE,PA,RO=1,2,4,8
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
def compress(cv,iw,ctr,flags,blen):
    # iw: up to 10 words -> m len16 with tail dups; returns state AND final m
    st=list(cv)+list(iw[:6])+[ctr&M,(ctr>>32)&M,flags,blen]
    m=list(iw)+[ctr&M,(ctr>>32)&M,flags,blen]  # len 14 when iw=10
    m=m+[m[0],m[1]]
    order=list(range(16))
    for r in range(7):
        mm=[m[order[i]] for i in range(16)]
        rnd(st,mm)
        order=[PERM[order[i]] for i in range(16)]
    return st,m
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
            rem=n-pos; take=min(64,max(0,rem)); blk=data[pos:pos+take]
            last=rem<=64
            f=(CS if pos==0 else 0)|((CE|(RO if is_root else 0)) if last else 0)
            st,m=compress(cv,wle(blk),c,f,take)
            cv=chaining(st,cv)
            if last: return cv,(list(st[8:16]),cv,c,f,take)
            c+=1;pos+=take
    chunks=(len(data)+1023)//1024
    p=1
    while p*2<chunks:p*=2
    pb=min(p*1024,len(data))
    cvL,_=output_node(data[:pb],key,counter,False)
    cvR,_=output_node(data[pb:],key,counter+p,False)
    cv=[cvL[0],cvL[1],cvL[2],cvL[3],cvR[0],cvR[1],cvR[2],cvR[3]]
    iw=cv[:8]  # [cvL[0..4),cvR[0..4)] = cv itself
    f=PA|(RO if is_root else 0)
    st,m=compress(cv,iw,0,f,64)
    chain=chaining(st,cv)
    return chain,(list(st[8:16]),chain,0,f,64)
def root_output(o):
    sched16,ctr,flags,blen=o[0],o[2],o[3],o[4]
    st=list(IV)+list(sched16)
    m=list(sched16)
    mm=[m[PERM[i]] for i in range(16)]
    rnd(st,mm)
    icv=[0]*8
    for i in range(4):
        icv[i]=(st[i]+st[i+8])&M
        icv[i+4]=(st[i+4]+st[i+12])&M
    out=[icv[i]^icv[i+4] for i in range(4)]
    return struct.pack('<8I',*out)
def hash256(data,key=None):
    _,o=output_node(bytes(data),key or IV,0,True)
    return root_output(o)[:32]
if __name__=="__main__":
    exp={0:"af1349b9f5f9a1a6",1:"2d3adedff11b61f1",2:"7b7015bb92cf0b31",5:"b40b44dfd97e7a84",10:"87fcf07cac5be3c9"}
    ok=True
    for ln,e in exp.items():
        h=hash256(bytes(i%251 for i in range(ln))).hex()
        print(ln,h[:16],"OK" if h.startswith(e) else "FAIL expect "+e)
        ok=ok and h.startswith(e)
    print("ALL OK" if ok else "STILL BROKEN")
