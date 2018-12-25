package com.summer.test;

import java.util.Scanner;

public class Test {
    public static void main(String[] args) {
        Scanner sca = new Scanner(System.in);
        for (int i = 1; i <= 5; i++) {
            System.out.println("请输入张三的第" + i + "门成绩");
            float score = sca.nextFloat();
        }
    }
}
