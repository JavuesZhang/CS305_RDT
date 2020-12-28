# CS305_RDT
SUSTech CS305 Computer Network 

2020-Fall Final Project 

Reliable Data Transfer (RDT)

<br>

### RDT Segment:
Format
```angular2html
      0   1   2   3   4   5   6   7   8   9   a   b   c   d   e   f
    +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
    |                          Source port #                        |
    +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
    |                            Dest port #                        |
    +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
    |                        Sequence number                        |
    |                                                               |
    +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
    |                     Acknowledgment number                     |
    |                                                               |
    +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
    | Header length |ACK|RST|SYN|FIN|         Unused                |
    +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
    |                           Checksum                            |
    +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
    |                                                               |
    /                            Options                            /
    /                                                               /
    +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
    |                                                               |
    /                            Payload                            /
    /                                                               /
    +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+

    Flags:
     - HEADER LENGTH             Header length, unit in word 
     - ACK                       Acknowledge
     - RST                       Reset
     - SYN                       Synchronize
     - FIN                       Finish


    Ranges:
     - Source port               0 - 65535
     - Dest port                 0 - 65535
     - Head Length               16 - 60          bytes
     - Sequence Number           0 - 4294967296   bytes
     - Acknowledgement Number    0 - 4294967296   bytes

    Checksum Algorithm:          16 bit one's complement of the one's complement sum
```

<br>

Options:
```angular2html
Kind: 5
Length: Variable
    
                      +--------+--------+
                      | Kind=5 | Length |
    +--------+--------+--------+--------+
    |      Left Edge of 1st Block       |
    +--------+--------+--------+--------+
    |      Right Edge of 1st Block      |
    +--------+--------+--------+--------+
    |                                   |
    /            . . .                  /
    |                                   |
    +--------+--------+--------+--------+
    |      Left Edge of nth Block       |
    +--------+--------+--------+--------+
    |      Right Edge of nth Block      |
    +--------+--------+--------+--------+

    Field:
     - Kind                       Optional types
     - Length                     The length of this option, in bytes
     - Left Edge of nth Block     The left byte range of retransmission requirements
     - Right Edge of nth Block    The right range of bytes of data to be retransmitted

    Block:
     - Maximum number             5

    Function: 
     - Carry the data sequence number range (in bytes) that requires peer retransmission
```





ref [RFC 793](https://tools.ietf.org/html/rfc793)
TCP Option summary: [TCP系列08—连接管理—7、TCP 常见选项(option)](https://www.cnblogs.com/lshs/archive/2004/01/13/6038494.html)

TCP D-SACK reference introduce: [TCP 的那些事 | D-SACK](https://blog.csdn.net/u014023993/article/details/85041321)

python struct 格式字符 https://docs.python.org/zh-cn/3.10/library/struct.html?highlight=struct#struct-format-strings

ref [RFC 2883](https://tools.ietf.org/html/rfc2883)
