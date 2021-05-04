package gr.athenarc.imsi.visualfacts;

public class DummyCategoricalColumn extends CategoricalColumn {

    int cardinality = 0;

    public DummyCategoricalColumn(int index, int cardinality) {
        super(index);
        this.cardinality = cardinality;
    }

    public int getCardinality() {
        return cardinality;
    }

}
