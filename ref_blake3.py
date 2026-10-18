import struct
IV=[0x6A09E667,0xBB67AE85,0x3C6EF372,0xA54FF53A,0x510E527F,0x9B05688C,0x1F83D9AB,0x5BE0CD19]
PERM=[2,6,3,10,7,0,4,13,1,11,12,5,9,14,15,8]
M=(1<<32)-1
CS,CE,P,R=1,2,4,8
def rotr(x,n): return ((x>>n)|(x<<(32-n)))&M
def g(s,a,b,c,d,mx,my):
    s[a]=(s[a]+s[b]+mx)&M; s[d]=rotr(s[d]^s[a],16); s[c]=(s[c]+s[d])&M; s[b]=rotr(s[b]^s[c],12)
    s[a]=(s[a]+s[b]+my)&M; s[d]=rotr(s[d]^s[a],8);  s[c]=(s[c]+s[d])&M; s[b]=rotr(s[b]^s[c],7)
def compress(cv,iw10,counter,flags,blen):
    st=cv[:]+iw10[:8]+[counter&M,(counter>>32)&M,flags,blen]
    m=iw10[:10]+[counter&M,(counter>>32)&M,flags,blen]
    order=list(range(16))
    for r in range(7):
        mm=[m[order[i]] for i in range(16)]
        g(st,0,4,8,12,mm[0],mm[1]); g(st,1,5,9,13,mm[2],mm[3]); g(st,2,6,10,14,mm[4],mm[5]); g(st,3,7,11,15,mm[6],mm[7])
        g(st,0,5,10,15,mm[8],mm[9]); g(st,1,6,11,12,mm[10],mm[11]); g(st,2,7,8,13,mm[12],mm[13]); g(st,3,4,9,14,mm[14],mm[15])
        order=[PERM[order[i]] for i in range(16)]
    cvout=[(st[i]+st[i+8])&M for i in range(4)]+[(st[i+4]+cv[i])&M for i in range(4)]
    return cvout,st
def output_node(data,key,counter,is_root):
    if len(data)<=1024:
        cv=key[:]; n=len(data); pos=0
        while True:
            rem=n-pos; take=min(64,max(0,rem))
            blk=data[pos:pos+take].ljust(64,b'\0')
            iw10=list(struct.unpack('<16I',blk))[:10]
            last=rem<=64
            flags=(CS if pos==0 else 0)|((CE|R) if (last and is_root) else (CE if last else 0))
            cv,st=compress(cv,iw10,counter,flags,take)
            if last: return cv,(iw10,counter,flags,take)
            counter+=1; pos+=take
    chunks=(len(data)+1023)//1024
    p=1
    while p*2<chunks: p*=2
    pb=min(p*1024,len(data))
    cvL,_=output_node(data[:pb],key,counter,False)
    cvR,_=output_node(data[pb:],key,counter+p,False)
    cv=cvL[:4]+cvR[:4]
    mix=cvL[:4]+cvR[:4]+[0,0]
    flags=P|(R if is_root else 0)
    cvout,st=compress(cv,mix,0,flags,64)
    return cvout,(mix,0,flags,64)
def hash256(data,key=None):
    k=key or IV
    cv,o=output_node(data,k,0,True)
    iw10,counter,flags,blen=o
    st=cv[:]+iw10[:8]+[counter&M,(counter>>32)&M,flags,blen]
    m=iw10[:10]+[counter&M,(counter>>32)&M,flags,blen]
    order=list(range(16))
    for r in range(7):
        mm=[m[order[i]] for i in range(16)]
        g(st,0,4,8,12,mm[0],mm[1]); g(st,1,5,9,13,mm[2],mm[3]); g(st,2,6,10,14,mm[4],mm[5]); g(st,3,7,11,15,mm[6],mm[7])
        g(st,0,5,10,15,mm[8],mm[9]); g(st,1,6,11,12,mm[10],mm[11]); g(st,2,7,8,13,mm[12],mm[13]); g(st,3,4,9,14,mm[14],mm[15])
        order=[PERM[order[i]] for i in range(16)]
    out=[(st[i]+st[i+8])&M for i in range(4)]+[(st[i+4]+cv[i])&M for i in range(4)]
    return struct.pack('<8I',*out).hex()
if __name__=="__main__":
    import blake3 as b3
    ok=True
    for L in [0,1,2,5,10,1023,1024,1025,2048,2080,3072,4096,5000,31774,100000]:
        data=bytes(i%251 for i in range(L))
        mine=hash256(data)[:24]; theirs=b3.blake3(data).hexdigest()[:24]
        print(L, mine, theirs, "OK" if mine==theirs else "MISMATCH")
        ok &= mine==theirs
    # tagged
    def tagged(tag,msg):
        tb=tag.encode(); pre=tb+len(tb).to_bytes(8,'little')+tb
        keyw=list(struct.unpack('<8I',bytes.fromhex(hash256(pre))))
        # need full CV words 0..7 of prehash = chaining value, not expanded output!
        cv,_=output_node(pre,IV,0,True)
        kw=cv[:]
        return hash256(msg,kw)
    t=tagged("domain separation",b"test")
    th = ""
    print("tagged",t[:24])
    print("ALL OK" if ok else "FAILURES")
