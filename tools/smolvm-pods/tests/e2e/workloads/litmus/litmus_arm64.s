#include "textflag.h"

// func writer(x, y *uint64, n uint64)
TEXT ·writer(SB),NOSPLIT,$0-24
	MOVD	x+0(FP), R0
	MOVD	y+8(FP), R1
	MOVD	n+16(FP), R2
	MOVD	$1, R3
wloop:
	MOVD	R3, (R0)
	MOVD	R3, (R1)
	ADD	$1, R3
	CMP	R2, R3
	BLS	wloop
	RET

// func reader(x, y *uint64, n, maxReads uint64) (reads, racing, violations uint64)
TEXT ·reader(SB),NOSPLIT,$0-56
	MOVD	x+0(FP), R0
	MOVD	y+8(FP), R1
	MOVD	n+16(FP), R2
	MOVD	maxReads+24(FP), R3
	MOVD	$0, R4	// reads
	MOVD	$0, R5	// violations
	MOVD	$0, R8	// racing
	MOVD	$0, R9	// y at the previous read
rloop:
	MOVD	(R1), R6	// y
	MOVD	(R0), R7	// x
	ADD	$1, R4
	CMP	R6, R7
	BHS	ordered
	ADD	$1, R5
ordered:
	CMP	R2, R6
	BHS	done	// the writer has finished
	CMP	R9, R6
	BEQ	next	// the writer has not stored since the previous read
	MOVD	R6, R9
	ADD	$1, R8
next:
	CMP	R3, R4
	BLO	rloop
done:
	MOVD	R4, reads+32(FP)
	MOVD	R8, racing+40(FP)
	MOVD	R5, violations+48(FP)
	RET
