#include "textflag.h"

// Fast goroutine-id support for amd64 (see goid_amd64.go). The runtime g
// pointer is read from TLS, and the goid field is read by offset so that all
// unsafe pointer arithmetic stays in assembly (Go-side checkptr/vet see none).

// func getg() unsafe.Pointer
TEXT ·getg(SB),NOSPLIT,$0-8
    MOVQ (TLS), AX
    MOVQ AX, ret+0(FP)
    RET

// func readGoid(base unsafe.Pointer, offset uintptr) int64
TEXT ·readGoid(SB),NOSPLIT,$0-24
    MOVQ base+0(FP), AX
    MOVQ offset+8(FP), CX
    ADDQ CX, AX
    MOVQ (AX), AX
    MOVQ AX, ret+16(FP)
    RET

// func scanGoid(base unsafe.Pointer, target int64, max uintptr) uintptr
TEXT ·scanGoid(SB),NOSPLIT,$0-32
    MOVQ base+0(FP), AX
    MOVQ target+8(FP), DX
    MOVQ max+16(FP), CX
    XORQ DI, DI
loop:
    CMPQ DI, CX
    JGE notfound
    MOVQ (AX)(DI*1), SI
    CMPQ SI, DX
    JEQ found
    ADDQ $8, DI
    JMP loop
found:
    MOVQ DI, ret+24(FP)
    RET
notfound:
    MOVQ $0, ret+24(FP)
    RET
