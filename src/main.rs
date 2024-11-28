
use std::fs::File;
use std::io::{self, BufRead};

use ascent::{ascent, ascent_par};
use dbsp::{
    mimalloc::MiMalloc, operator::Generator, typed_batch::DynBatchReader, utils::Tup2, Circuit,
    OrdZSet, RootCircuit, Runtime, Stream,
};

#[global_allocator]
static ALLOC: MiMalloc = MiMalloc;

fn read_edge(f: &'static str) -> Vec<(u32, u32)> {
    let file = File::open(f).unwrap();
    let reader = io::BufReader::new(file);
    let mut tuples = Vec::new();
    for line in reader.lines() {
        let line = line.unwrap();
        let mut parts = line.split("\t");
        let from = parts.next().unwrap().parse::<u32>().unwrap();
        let to = parts.next().unwrap().parse::<u32>().unwrap();
        tuples.push((from, to));
    }
    tuples
}

fn run_dbsp(f: &'static str) {
    let hruntime = Runtime::run(16, move || {
        let circuit = RootCircuit::build(|circuit| {
            // const LAYER: u32 = 200;
            let tuples = read_edge(f).into_iter().map(|(x, y)| Tup2(Tup2(Tup2(x, y), ()), 1)).collect();
            // let mut tuples = Vec::new();
            // if Runtime::worker_index() == 0 {
            //     for layer in 0..5 {
            //         for from in 0..LAYER {
            //             for to in 0..LAYER {
            //                 tuples.push(Tup2(
            //                     Tup2(Tup2(from + (LAYER * layer), to + LAYER * (layer + 1)), ()),
            //                     1,
            //                 ));
            //             }
            //         }
            //     }
            // }

            let edges = <OrdZSet<Tup2<u32, u32>>>::from_tuples((), tuples);

            let edges: Stream<_, OrdZSet<Tup2<u32, u32>>> =
                circuit.add_source(Generator::new(move || edges.clone()));

            let paths = circuit
                .recursive(|child, paths: Stream<_, OrdZSet<Tup2<u32, u32>>>| {
                    // ```text
                    //                            distinct
                    //               ┌───┐          ┌───┐
                    // edges         │   │          │   │  paths
                    // ────┬────────►│ + ├──────────┤   ├────────┬───►
                    //     │         │   │          │   │        │
                    //     │         └───┘          └───┘        │
                    //     │           ▲                         │
                    //     │           │                         │
                    //     │         ┌─┴─┐                       │
                    //     │         │   │                       │
                    //     └────────►│ X │ ◄─────────────────────┘
                    //               │   │
                    //               └───┘
                    //               join
                    // ```
                    let edges = edges.delta0(child);

                    let paths_inverted = paths.map(|&Tup2(x, y)| Tup2(y, x));

                    let paths_inverted_indexed = paths_inverted.map_index(|Tup2(k, v)| (*k, *v));
                    let edges_indexed = edges.map_index(|Tup2(k, v)| (*k, *v));

                    Ok(edges.plus(
                        &paths_inverted_indexed
                            .join(&edges_indexed, |_via, from, to| Tup2(*from, *to)),
                    ))
                })
                .unwrap();

            paths.gather(0).inspect(|zs: &OrdZSet<_>| {
                if Runtime::worker_index() == 0 {
                    println!("paths: {}", zs.len())
                }
            });
            Ok(())
        })
        .unwrap()
        .0;

        //let graph = monitor.visualize_circuit();
        //fs::write("path.dot", graph.to_dot()).unwrap();

        circuit.step().unwrap();
    })
    .expect("runtime initialization should succeed");

    let start = std::time::Instant::now();
    hruntime.join().unwrap();
    let duration = start.elapsed();
    println!("Time elapsed in DBSP: {:?}", duration);
}

ascent_par!{
    struct TC;
    relation edge(u32, u32);
    relation path(u32, u32);

    path(x, y) <-- edge(x, y);
    path(x, z) <-- edge(x, y), path(y, z);
}

fn run_ascent_tc(f: &'static str) {
    let mut prog = TC::default();
    let input = read_edge(f);

    prog.edge = input.clone().into_iter().collect();

    // measure the time
    let start = std::time::Instant::now();
    prog.run();
    let duration = start.elapsed();
    println!("Time elapsed in Ascent TC: {:?}", duration);
    println!("edges: {}", prog.edge.len());
    println!("path: {}", prog.path.len());
}


fn main() {
    let test_input = "./data/fe_body/edge.facts";

    run_dbsp(test_input);
    run_ascent_tc(test_input);
}

