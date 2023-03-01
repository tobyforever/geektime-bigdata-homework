package org.example;

public class Person {
//    班级  姓名  年龄  性别  科目 成绩
//12 宋江 25 男 chinese 50
    int classname;

    public int getClassname() {
        return classname;
    }

    public void setClassname(int classname) {
        this.classname = classname;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public int getScore() {
        return score;
    }

    public void setScore(int score) {
        this.score = score;
    }

    String name;
    int age;
    String sex;
    String subject;
    int score;
}
