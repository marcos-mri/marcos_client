#/usr/bin/env python3
# Machine code defines and functions for flocra.
#
# Functions should be fast, without any floating-point arithmetic -
# that should be handled at a higher level.

class FloUserWarning(UserWarning):
    pass

class FloCompileWarning(FloUserWarning):
    pass

class FloGradWarning(FloUserWarning):
    pass

class FloServerWarning(FloUserWarning):
    pass

INOP = 0x0
IFINISH = 0x1
IWAIT = 0x2
ITRIG = 0x3
ITRIGFOREVER=0x4
IDATA = 0x80

GRAD_CTRL = 0
GRAD_LSB = 1
GRAD_MSB = 2
RX0_RATE = 3
RX1_RATE = 4
TX0_I = 5
TX0_Q = 6
TX1_I = 7
TX1_Q = 8
DDS0_PHASE_LSB = 9
DDS0_PHASE_MSB = 10
DDS1_PHASE_LSB = 11
DDS1_PHASE_MSB = 12
DDS2_PHASE_LSB = 13
DDS2_PHASE_MSB = 14
GATES_LEDS = 15
RX_CTRL = 16
FLOCRA_BUFS = RX_CTRL + 1

STATE_IDLE = 0
STATE_PREPARE = 1
STATE_RUN = 2
STATE_COUNTDOWN = 3
STATE_TRIG = 4
STATE_TRIG_FOREVER = 5
STATE_HALT = 8

COUNTER_MAX = 0xffffff

def insta(instr, data):
    """ Instruction A: FSM control """
    assert instr in [INOP, IFINISH, IWAIT, ITRIG, ITRIGFOREVER], "Unknown instruction"
    assert (data & COUNTER_MAX) == (data & 0xffffffff), "Data out of range"
    return (instr << 24) | (data & 0xffffff)
    
def instb(tgt, delay, data):
    """ Instruction B: timed buffered data """
    assert tgt <= 24, "Unknown target buffer"
    assert 0 <= delay <= 255, "Delay out of range"
    assert (data & 0xffff) == (data & 0xffffffff), "Data out of range"
    return (IDATA << 24) | ( (tgt & 0x7f) << 24 ) | ( (delay & 0xff) << 16 ) | (data & 0xffff)

# def set_ocra1(word, channel, broadcast=False, delay=0):
#     """ Assumes word, channel, delay and broadcast have already been sanitised """
#     word_full = (word << 2) | 0x00100000 | (channel << 25) | (broadcast << 24)
#     word_msb, word_lsb = word_full >> 16, word_full & 0xffff

#     instrs = []
#     instrs.append(instb(GRAD_LSB, word_lsb, delay + 1))
#     instrs.append(instb(GRAD_MSB, word_msb, delay))
#     return instrs


# def set_gpa_fhdo(word, channel, broadcast=False, delay=0):
#     """ Assumes word, channel, and delay have already been sanitised """
#     word_full = word | 0x80000 | (channel << 16) | (broadcast << 24) # 2 channels in the word
#     word_msb, word_lsb = word_full >> 16, word_full & 0xffff

#     instrs = []
#     instrs.append(instb(GRAD_LSB, word_lsb, delay + 1))
#     instrs.append(instb(GRAD_MSB, word_msb, delay))
#     return instrs

# def set_grad(*args, gb=grad_board, **kwargs):
#     if gb == "gpa-fhdo":
#         return set_gpa_fhdo(args, kwargs)
#     elif gb == "ocra1":
#         return set_ocra1(args, kwargs)
#     else:
#         warnings.warn("Undefined grad board")

