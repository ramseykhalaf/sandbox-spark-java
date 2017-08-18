import java.io.Serializable;

public class PersonV2 implements Serializable {
    private int age;
    private int id;
    private String name;
    private String country;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PersonV2)) return false;

        PersonV2 personV2 = (PersonV2) o;

        if (getAge() != personV2.getAge()) return false;
        if (getId() != personV2.getId()) return false;
        if (getName() != null ? !getName().equals(personV2.getName()) : personV2.getName() != null) return false;
        return getCountry() != null ? getCountry().equals(personV2.getCountry()) : personV2.getCountry() == null;
    }

    @Override
    public int hashCode() {
        int result = getAge();
        result = 31 * result + getId();
        result = 31 * result + (getName() != null ? getName().hashCode() : 0);
        result = 31 * result + (getCountry() != null ? getCountry().hashCode() : 0);
        return result;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public PersonV2() {
    }

    public PersonV2(int age, int id, String name, String country) {
        this.age = age;
        this.id = id;
        this.name = name;
        this.country = country;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public int getAge() {
        return age;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setAge(int age) {
        this.age = age;
    }

}
