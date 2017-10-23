package test.pool;

public enum TaskState {
    STARTED(0),
    WAITING(1),
    SUCCESS(2),
    FAILED(-1),
    KILLED(-2);

    private int value;

    private TaskState(int state) {
        this.value = state;
    }

    public int getValue() {
        return value;
    }


}
