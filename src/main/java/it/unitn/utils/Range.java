package it.unitn.utils;

public record Range(int gt, int lte) {

    @Override
    public String toString() {
        return "(%d, %d]".formatted(gt, lte);
    }

}
