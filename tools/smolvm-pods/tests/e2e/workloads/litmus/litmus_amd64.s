#include "textflag.h"

// func writer(x, y *uint64, n uint64)
TEXT ·writer(SB),NOSPLIT,$0-24
	MOVQ	x+0(FP), AX
	MOVQ	y+8(FP), BX
	MOVQ	n+16(FP), CX
	MOVQ	$1, DX
wloop:
	MOVQ	DX, (AX)
	MOVQ	DX, (BX)
	INCQ	DX
	CMPQ	DX, CX
	JLS	wloop
	RET

// func reader(x, y *uint64, n, maxReads uint64) (reads, racing, violations uint64)
TEXT ·reader(SB),NOSPLIT,$0-56
	MOVQ	x+0(FP), AX
	MOVQ	y+8(FP), BX
	MOVQ	n+16(FP), CX
	MOVQ	maxReads+24(FP), DX
	XORQ	SI, SI	// reads
	XORQ	DI, DI	// violations
	XORQ	R10, R10	// racing
	XORQ	R11, R11	// y at the previous read
rloop:
	MOVQ	(BX), R8	// y
	MOVQ	(AX), R9	// x
	INCQ	SI
	CMPQ	R9, R8
	JCC	ordered
	INCQ	DI
ordered:
	CMPQ	R8, CX
	JCC	done	// the writer has finished
	CMPQ	R8, R11
	JEQ	next	// the writer has not stored since the previous read
	MOVQ	R8, R11
	INCQ	R10
next:
	CMPQ	SI, DX
	JCS	rloop
done:
	MOVQ	SI, reads+32(FP)
	MOVQ	R10, racing+40(FP)
	MOVQ	DI, violations+48(FP)
	RET
