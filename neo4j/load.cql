// all csv files live in dbms.directories.import (<NEO4J_HOME>/import/*.csv by default)

// load organism nodes
CREATE CONSTRAINT ON (o:Organism) ASSERT o.id IS UNIQUE;
USING PERIODIC COMMIT 500
LOAD CSV WITH HEADERS FROM 'file:///organisms.csv' AS line
CREATE (o:Organism {id: line.id, genus: line.genus, species: line.species})
RETURN o;

// load chromosome nodes
CREATE CONSTRAINT ON (c:Chromosome) ASSERT c.id IS UNIQUE;
USING PERIODIC COMMIT 500
LOAD CSV WITH HEADERS FROM 'file:///chromosomes.csv' AS line
MATCH (o:Organism) WHERE o.id = line.organism
CREATE (c:Chromosome {id: line.id, name: line.name, length: toInteger(line.length)}),
       (c)-[c2o:ChromosomeToOrganism]->(o)
RETURN c, c2o;

// load gene family nodes
CREATE CONSTRAINT ON (f:GeneFamily) ASSERT f.id IS UNIQUE;
USING PERIODIC COMMIT 500
LOAD CSV WITH HEADERS FROM 'file:///families.csv' AS line
CREATE (f:GeneFamily {id: line.id, name: line.name})
RETURN f;

// load gene nodes
CREATE CONSTRAINT ON (g:Gene) ASSERT g.id IS UNIQUE;
USING PERIODIC COMMIT 500
LOAD CSV WITH HEADERS FROM 'file:///genes.csv' AS line
MATCH (f:GeneFamily) WHERE f.id = line.family
MATCH (c:Chromosome) WHERE c.id = line.chromosome
CREATE (g:Gene {id: line.id, name: line.name, fmin: toInteger(line.fmin), fmax: toInteger(line.fmax), strand: toInteger(line.strand), number: toInteger(line.number)}),
       (g)-[g2f:GeneToGeneFamily]->(f),
       (g)-[g2c:GeneToChromosome]->(c)
RETURN g, g2f, g2c;
